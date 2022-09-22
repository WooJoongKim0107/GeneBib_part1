from .cached import MetaCache


class Foo(metaclass=MetaCache):
    def __new__(cls, title, caching=True):
        """asdfasdf"""
        return super().__new__(cls)

    def __init__(self, title, caching=True):
        """asdfasdf"""
        self.title = title

    def __getnewargs__(self):
        return self.title, False


assert Foo(1) is Foo(1)
assert Foo(1) is not Foo(2)
assert Foo(2) is Foo(2)
