import pickle


class MetaLoad(type):
    LOAD_PATH = ''

    def __new__(mcs, name, bases, attrs):
        assert len(bases) == 1, 'Number of base classes of MetaLoad must be 1'
        assert ('LOAD_PATH' in attrs) or any(hasattr(base, 'LOAD_PATH') for base in bases), \
            "'LOAD_PATH' must be defined for MetaLoad"
        assert ('generate' in attrs) or any(hasattr(base, 'generate') for base in bases),\
            "'generate(cls)' must be defined for MetaLoad"
        self = super().__new__(mcs, name, bases, attrs)
        return self

    def load(cls):
        with open(cls.LOAD_PATH, 'rb') as file:
            return cls(pickle.load(file))

    def build(cls):
        self = cls.generate()
        primitive = cls.mro()[-2](self)
        with open(cls.LOAD_PATH, 'wb') as file:
            pickle.dump(primitive, file)
        return self

    def generate(cls):
        raise NotImplementedError
        # return cls()
