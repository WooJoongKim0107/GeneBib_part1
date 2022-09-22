import gzip
import pickle
from functools import wraps


def deco_new(new):
    @wraps(new)
    def wrap_new(cls, key, caching=True):
        if caching and (key in cls.CACHE):
            return cls.CACHE[key]
        return new(cls, key)
    return wrap_new


def deco_init(init):
    @wraps(init)
    def wrap_init(self, key, caching=True):
        if caching and (key in self.CACHE):
            return
        elif caching:
            self.CACHE[key] = self
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
        self.CACHE = {}
        self.CACHE_PATH = attrs.get('CACHE_PATH', '')
        return self

    def export_cache(self, path=None):
        print(self)
        path = path if path else self.CACHE_PATH
        with gzip.open(path, 'wb') as file:
            pickle.dump(self.CACHE, file)

    def import_cache(self, path=None, verbose=True):
        path = path if path else self.CACHE_PATH
        if verbose:
            print(f'Import {self.__name__} cache from:', path)
        self.CACHE.clear()
        with gzip.open(path, 'rb') as file:
            self.CACHE.update(pickle.load(file))

    def import_cache_if_empty(self, path=None, verbose=True):
        path = path if path else self.CACHE_PATH
        if not self.CACHE:
            if verbose:
                print(f'Import {self.__name__} cache from:', path)
            self.CACHE.clear()
            with gzip.open(path, 'rb') as file:
                self.CACHE.update(pickle.load(file))

    def merge_caches(self, *caches):
        assert 'merge' in dir(self), f'Merging {self.__name__} cache failed: {self.__name__}.merge does not exist.'
        anchor = caches[0].copy()
        for cache in caches[1:]:
            self._update_cache(anchor, cache)
        self.CACHE.clear()
        self.CACHE.update(anchor)

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
        return cls.CACHE.__len__()

    def __iter__(cls):
        return cls.CACHE.__iter__()

    def __getitem__(cls, k):
        return cls.CACHE.__getitem__(k)

    def keys(cls):
        return cls.CACHE.keys()

    def values(cls):
        return cls.CACHE.values()

    def items(cls):
        return cls.CACHE.items()


if __name__ == '__main__':
    from multiprocessing import Pool
    from mypathlib import PathTemplate


    class Foo(metaclass=MetaCacheExt):
        CACHE_PATH = PathTemplate('$base/foo_cache.pkl').substitute()

        def __new__(cls, key, caching=True):
            """__new__ method must not be skipped - Assertion of MetaCacheExt"""
            return super().__new__(cls)

        def __init__(self, key, caching=True):
            self.key = key
            self.x = set()

        def __getnewargs__(self):
            """__getnewargs__ must not be deleted - Assertion of MetaCacheExt"""
            return self.key, False

        def merge(self, other):
            """Method to merge another instance to itself
            This method must not be deleted or renamed - Assertion of MetaCacheExt"""
            self.x.update(other.x)


    def pioneer():
        for i in range(100):
            Foo(i).x.add(i)
        return Foo.CACHE


    def part1(x):
        Foo(x).x.add(x**2)
        return Foo.CACHE


    def part2(x):
        Foo(x).x.add(x**3)
        return Foo.CACHE


    def main():
        pioneer()
        # Foo(i).x == {i}
        with Pool(5) as p:
            caches = p.map(part1, range(100))
        # Foo(i).x == {i} in main process, but caches[i][i].x == {i, i**2}
        Foo.merge_caches(*caches)
        # Foo(i).x == {i, i**2}
        with Pool(5) as p:
            caches = p.map(part2, range(100))
        # Foo(i).x == {i, i**2} in main process, but caches[i][i].x == {i, i**2, i**3}
        Foo.merge_caches(*caches)
        # Foo(i).x == {i, i**2, i**3}
        Foo.export_cache()
