from functools import wraps
import pickle, gzip


def deco_new(new):
    @wraps(new)
    def wrap_new(cls, key, caching=True):
        if caching and (key in cls._CACHE):
            return cls._CACHE[key]
        return new(cls, key)
    return wrap_new


def deco_init(init):
    @wraps(init)
    def wrap_init(self, key, caching=True):
        if caching and (key in self._CACHE):
            return
        elif caching:
            self._CACHE[key] = self
        init(self, key)
    return wrap_init


class MetaCache(type):
    def __new__(mcs, name, bases, attrs):
        assert '__new__' in attrs, '__new__ must be implemented.'
        assert '__init__' in attrs, '__init__ must be implemented.'
        assert '__getnewargs__' in attrs, '__getnewargs__ must be implemented.'

        attrs['__new__'] = deco_new(attrs['__new__'])
        attrs['__init__'] = deco_init(attrs['__init__'])
        self = super().__new__(mcs, name, bases, attrs)
        self._CACHE = {}
        self._CACHE_PATH = attrs.get('_CACHE_PATH', '')
        return self

    def export_cache(self, path=None):
        print(self)
        path = path if path else self._CACHE_PATH
        with gzip.open(path, 'wb') as file:
            pickle.dump(self._CACHE, file)

    def import_cache(self, path=None, verbose=True):
        path = path if path else self._CACHE_PATH
        if verbose:
            print(f'Import {self.__name__} cache from:', path)
        with gzip.open(path, 'rb') as file:
            self._CACHE = pickle.load(file)

    def merge_caches(self, *caches):
        assert 'merge' in dir(self), f'Merging {self.__name__} cache failed: {self.__name__}.merge does not exist.'
        anchor = caches[0].copy()
        for cache in caches[1:]:
            self._update_cache(anchor, cache)
        self._CACHE = anchor

    @staticmethod
    def _update_cache(c0: dict, c1: dict):
        intersections = c0.keys() & c1.keys()
        for key in intersections:
            c0[key].merge(c1[key])

        for k1, j1 in c1.items():
            if k1 not in intersections:
                c0[k1] = j1


class MetaCacheExt(MetaCache):
    def __len__(cls):
        return cls._CACHE.__len__()

    def __iter__(cls):
        return cls._CACHE.__iter__()

    def __getitem__(cls, k):
        return cls._CACHE.__getitem__(k)

    def keys(cls):
        return cls._CACHE.keys()

    def values(cls):
        return cls._CACHE.values()

    def items(cls):
        return cls._CACHE.items()
