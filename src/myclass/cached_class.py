from functools import wraps

class MetaCache(type):
    def __new__(mcs, name, bases, attrs):
        assert '__getnewargs__' in attrs, '__getnewargs__ must be implemented.'
        attrs['__init__'] = deco_init(attrs['__init__'])
        attrs['__new__'] = deco_new(attrs['__new__'])
        self = super().__new__(mcs, name, bases, attrs)
        self._CACHE = {}
        return self


def deco_new(new):
    @wraps(new)
    def wrap_new(cls, title):
        if title in cls._CACHE:
            return cls._CACHE[title]
        return new(cls, title)

    return wrap_new


def deco_init(init):
    @wraps(init)
    def wrap_init(self, title):
        if title in self._CACHE:
            return
        self._CACHE[title] = self
        init(self, title)

    return wrap_init


if __name__ == '__main__':
    class Foo(metaclass=MetaCache):
        def __new__(cls, title):
            """asdfasdf"""
            return super().__new__(cls)

        def __init__(self, title):
            """asdfasdf"""
            self.title = title

    assert Foo(1) is Foo(1)
    assert Foo(1) is not Foo(2)
    assert Foo(2) is Foo(2)
