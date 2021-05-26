import io
import os
import hashlib
import requests
import urllib
import paramiko
import ftplib

from io import TextIOWrapper

from engine.wprdc_etl.pipeline.exceptions import HTTPConnectorError
from engine.parameters.google_api_credentials import PATH_TO_SERVICE_ACCOUNT_JSON_FILE, GCP_BUCKET_NAME # These imports
# are the first instance of crossing over the boundary between wprdc-etl and rocket-etl.
# These libraries could be kept more isolated by passing such parameters through the
# calls to the pipeline code, but this is smoother.
# [ ] This paves the way to eliminate settings.json, which is in a different format and
# location than all the other parameters/credentials.

SFTP_MAX_FILE_SIZE = 500000 #KiB

class Connector(object):
    '''Base connector class.

    Subclasses must implement ``connect``, ``checksum_contents``,
    and ``close`` methods.
    '''
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.get('encoding', 'utf-8')
        self.local_cache_filepath = kwargs.get('local_cache_filepath', None) # This
        # tells non-local connectors where to cache retrieved files.
        self.verify_requests = kwargs.get('verify_requests', True)
        self.checksum = None

    def connect(self, target):
        '''Base connect method

        Should return an object that can be iterated through via
        the :py:func:`next` builtin method.
        '''
        raise NotImplementedError

    def checksum_contents(self, target):
        '''Should return an md5 hash of the contents of the conn object
        '''
        raise NotImplementedError

    def close(self):
        '''Teardown any open connections (like to a file, for example)
        '''
        raise NotImplementedError

class FileConnector(Connector):
    '''Base connector for file objects.
    '''
    def connect(self, target):
        '''Connect to a file

        Runs :py:func:`open` on the passed ``target``, sets the
        result on the class as ``_file``, and returns it.

        Arguments:
            target: a valid filepath

        Returns:
            A `file-object`_
        '''
        if self.encoding and self.encoding != 'binary':
            self._file = open(target, 'r', encoding=self.encoding)
        else:
            self._file = open(target, 'rb')
        return self._file

    def checksum_contents(self, target, blocksize=8192):
        '''Open a file and get a md5 hash of its contents

        Arguments:
            target: a valid filepath

        Keyword Arguments:
            blocksize: the size of the block to read at a time
                in the file. Defaults to 8192.

        Returns:
            A hexadecimal representation of a file's contents.
        '''
        _file = self._file if self._file else self.connect(target)
        m = hashlib.md5()
        for chunk in iter(lambda: _file.read(blocksize, ), b''):
            if not chunk:
                break
            m.update(chunk.encode(self.encoding) if self.encoding else chunk)
        self._file.seek(0)
        return m.hexdigest()

    def close(self):
        '''Closes the connected file if it is not closed already
        '''
        if not self._file.closed:
            self._file.close()
        return

class GoogleCloudStorageFileConnector(FileConnector):
    '''Connector for a file located in a Google Cloud Storage bucket.
    '''
    def connect(self, target):
        '''Connect to a Google Cloud Storage bucket

        Arguments:
            target: Blob name

        Returns:
            :py:class:`io.TextIOWrapper` around the opened URL unless
            the encoding is 'binary', in which case it returns
            a `io.BufferedReader` instance.
        '''
        from google.cloud import storage

        storage_client = storage.Client.from_service_account_json(PATH_TO_SERVICE_ACCOUNT_JSON_FILE)
        blobs = list(storage_client.list_blobs(GCP_BUCKET_NAME))
        possible_blobs = [b for b in blobs if b.name == target]
        if len(possible_blobs) != 1:
            raise RuntimeError(f'{len(possible_blobs)} blobs found with the file name {target}.')

        blob = possible_blobs[0]

        if self.encoding == 'binary':
            self._file = blob.open()
        else:
            self._file = blob.open(encoding=self.encoding)
        return self._file

class RemoteFileConnector(FileConnector):
    '''Connector for a file located at a remote (HTTP-accessible) resource

    This class should be used to connect to a file available over
    HTTP. For example, if there is a CSV that is streamed from a
    web server, this is the correct connector to use.
    '''
    def connect(self, target):
        '''Connect to a remote target

        Arguments:
            target: Remote URL

        Returns:
            :py:class:`io.TextIOWrapper` around the opened URL unless
            the encoding is 'binary', in which case it returns
            a `io.BufferedReader` instance.
        '''
        if self.encoding == 'binary':
            self._file = io.BytesIO(requests.get(target, verify=self.verify_requests).content)
        else:
            self._file = TextIOWrapper(urllib.request.urlopen(target), encoding=self.encoding)
        return self._file

class HTTPConnector(Connector):
    ''' Connect to remote file via HTTP
    '''
    # It looks like RemoteFileConnector is designed to handle a file served by a web server,
    # while HTTPConnector is designed to pull text or JSON from a web server.
    def connect(self, target):
        response = requests.get(target)
        if response.status_code > 299:
            raise HTTPConnectorError(
                'Request could not be processed. Status Code: ' +
                str(response.status_code)
            )

        if 'application/json' in response.headers['content-type']:
            return response.json()

        return response.text

    def close(self):
        return True

class SFTPConnector(FileConnector):
    ''' Connect to remote file via SFTP
    '''
    def __init__(self, *args, **kwargs):
        super(SFTPConnector, self).__init__(*args, **kwargs)
        self.host = kwargs.get('host', None)
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.port = kwargs.get('port', 22)
        self.root_dir = kwargs.get('root_dir', '').rstrip('/') + '/'
        self.conn, self.transport, self._file = None, None, None

    def connect(self, target):
        try:
            self.transport = paramiko.Transport((self.host, self.port))
            #self.transport.timeout = 180 # This is where and how to increase the timeouts to deal with a slow FTP server.
            #self.transport.banner_timeout = 180
            #self.transport.auth_timeout = 180
            self.transport.connect(
                username=self.username, password=self.password
            )
            self.conn = paramiko.SFTPClient.from_transport(self.transport)
            size = self.conn.stat(self.root_dir + target).st_size
            if self.conn.stat(self.root_dir + target).st_size > SFTP_MAX_FILE_SIZE:
                # For large files, copy to local folder first
                # prevents re-downloading data for checksum and extraction
                if self.local_cache_filepath is not None:
                    local_cache_filepath = self.local_cache_filepath
                else:
                    local_cache_filepath = os.path.basename(target) # This can just be a filename,
                    # making this filepath relative to the directory the script is run from.
                self.conn.get(self.root_dir + target, local_cache_filepath)
                return super(SFTPConnector, self).connect(local_cache_filepath)
            else:
                self._file = io.BytesIO(self.conn.open(self.root_dir + target, 'r').read())

            if self.encoding:
                self._file = io.TextIOWrapper(self._file, self.encoding)

        except IOError as e:
            raise e

        return self._file

    def close(self):
        self.conn.close()
        self.transport.close()
        if not self._file.closed:
            self._file.close()


class FTPConnector(FileConnector):
    ''' Connect to remote file via SFTP
    '''
    def __init__(self, *args, **kwargs):
        super(FTPConnector, self).__init__(*args, **kwargs)
        self.host = kwargs.get('host', None)
        if self.host is None: # self.host comes from settings.json
            self.host = kwargs.get('fallback_host', None) # fallback_host comes from source_site from the job_dict.
        self.username = kwargs.get('username', 'anonymous')
        self.password = kwargs.get('password', 'anonymous')
        self.passive = kwargs.get('passive', False)
        self.ftp = None
        self.file_text = ''

    def connect(self, target):
        try:
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login(self.username, self.password)
            self.ftp.set_pasv(self.passive)
            if self.encoding != 'binary':
                self.ftp.retrlines('RETR ' + target, self.add_to_file)
                if 'latin-sig' in self.encoding:
                    b = self.file_text.encode('latin-1')
                    self.file_text = b.decode('utf-8-sig')
                self._file = io.StringIO(self.file_text)
            else:
                self._file = io.BytesIO()
                self.ftp.retrbinary('RETR ' + target, self._file.write)
                self._file.seek(0)

        except IOError as e:
            raise e

        return self._file

    def close(self):
        try:
            self.ftp.quit()
        except:
            pass
        if not self._file.closed:
            self._file.close()

    def add_to_file(self, line):
        self.file_text += line + "\n"
