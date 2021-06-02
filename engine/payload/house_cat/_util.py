from operator import itemgetter as i
from functools import cmp_to_key

def cmp(x, y):
    """
    Replacement for built-in function cmp that was removed in Python 3

    Compare the two objects x and y and return an integer according to
    the outcome. The return value is negative if x < y, zero if x == y
    and strictly positive if x > y.

    https://portingguide.readthedocs.io/en/latest/comparisons.html#the-cmp-function
    """
    if x is None:
        x = ''
    if y is None:
        y = ''
    return (x > y) - (x < y)

def pad(d, keys):
    new_d = dict(d)
    for key in keys:
        if key not in new_d:
            new_d[key] = None
    return new_d

def pad_with_empty_fields(ds):
    import itertools
    unique_keys = list(set(itertools.chain(*[list(d.keys()) for d in ds])))
    padded_ds = [pad(d, unique_keys) for d in ds]
    return padded_ds

def multikeysort(unpadded_items, columns):
    items = pad_with_empty_fields(unpadded_items)
    comparers = [
        ((i(col[1:].strip()), -1) if col.startswith('-') else (i(col.strip()), 1))
        for col in columns
    ]
    def comparer(left, right):
        comparer_iter = (
            cmp(fn(left), fn(right)) * mult
            for fn, mult in comparers
        )
        return next((result for result in comparer_iter if result), 0)
    return sorted(items, key=cmp_to_key(comparer))
