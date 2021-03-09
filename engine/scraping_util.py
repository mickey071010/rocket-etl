from bs4 import BeautifulSoup
import requests, re

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def make_relative_url_absolute(nth_url, web_page_url):
    if re.match('http', nth_url) is not None:
        return nth_url
    if re.match('/', nth_url) is None:
        return '/'.join(web_page_url.split('/')[:-1]) + '/' + nth_url
    return '/'.join(web_page_url.split('/')[:3]) + nth_url

def scrape_nth_link(web_page_url, extension, n, expected_matches, regex=None, verify=True):
    """Function for scraping a web page for links to a particular kind of file
    (with extension 'extension'). It verifies that the number of such links
    is equal to the expected number (to catch some cases where the web page
    has changed) and then returns the nth link. As a second validation stage,
    it optionally requires that the extracted URL match a provided regex.
    Other validation (such as requiring that the link text match a given regex
    or record-level validation) might be a good idea.

    Set verify = False to route around web sites with poorly configured
    certificates.
    """
    r = requests.get(web_page_url, verify=verify)
    soup = BeautifulSoup(r.text, 'html.parser')

    doc_urls = []
    for link in soup.find_all('a'):
        url = link.get('href', 'No Link Found')
        if re.search(f".{extension}$", url) is not None:
            doc_urls.append(url)

    if expected_matches is not None:
        assert len(doc_urls) == expected_matches

    nth_url = doc_urls[n]
    if regex is not None:
        assert re.search(regex, nth_url) is not None
    nth_url = make_relative_url_absolute(nth_url, web_page_url)
    return nth_url
