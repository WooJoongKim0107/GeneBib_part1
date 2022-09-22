class MetaDisposal(type):
    DATA = None

    def __new__(mcs, name, bases, attrs):
        assert 'load' in attrs, 'load() method must be implemented'
        assert 'DATA' in attrs, 'DATA attribute must be defined'
        self = super().__new__(mcs, name, bases, attrs)
        return self

    def load(cls):
        raise NotImplementedError(f"What's wrong with {cls}?")

    def safe_load(cls):
        if not cls.DATA:
            cls.load()
