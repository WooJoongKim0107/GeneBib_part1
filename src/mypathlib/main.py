__all__ = ['PathTemplate', 'TempOpen']

from string import Template
from pathlib import Path
from collections import ChainMap
from collections.abc import Callable, Mapping

_sentinel_dict = {}


class PathTemplate(Template):
    _base = Path(__file__).parents[2]
    _rsrc = _base / 'rsrc'

    def __init__(self, template, key=None):
        self._template = template
        self._key = key
        new_template = Template(template).safe_substitute(base=self._base, rsrc=self._rsrc)
        super().__init__(new_template)

    def __repr__(self):
        return f"PathTemplate('{self.template}')"

    def __add__(self, other):
        return type(self)(self.template.__add__(other))

    def substitute(self, mapping=_sentinel_dict, /, key=None, **kwargs) -> Path:
        subs_dict = self._get_subs_dict(mapping, key=key, **kwargs)
        return Path(super().substitute(subs_dict))

    def safe_substitute(self, mapping=_sentinel_dict, /, key=None, **kwargs):
        subs_dict = self._get_subs_dict(mapping, key=key, **kwargs)
        res: str = super().safe_substitute(subs_dict)
        if self.delimiter not in res:
            return Path(res)
        else:
            return PathTemplate(res)

    def private_substitute(self, mapping=_sentinel_dict, /, key=None, **kwargs) -> str:
        subs_dict = self._get_subs_dict(mapping, key=key, **kwargs)
        return Template(self._template).safe_substitute(subs_dict)

    def _get_subs_dict(self, mapping=_sentinel_dict, /, key=None, **kwargs):
        _key = self._chain(key)
        _mapping = self._merge(mapping, kwargs)
        return self._apply(_mapping, _key)

    def _chain(self, key):
        match (self._key, key):
            case (None, k) | (k, None):
                return k
            case (Callable() as f, Mapping() as dct) | (Mapping() as dct, Callable() as f):
                return _DefaultDict(dct, f)
            case (Callable(), Callable() as f):
                return f
            case (Mapping() as dct0, Mapping() as dct1):
                return ChainMap(dct1, dct0)
            case _:
                raise NotImplementedError(f"{type(self).__name__}._chain(key) encountered invalid type for 'key'")

    @classmethod
    def _merge(cls, mapping: dict, kwargs: dict):
        if mapping is _sentinel_dict:
            return kwargs
        elif kwargs and isinstance(mapping, Mapping):
            return ChainMap(kwargs, mapping)
        else:
            raise TypeError(f'position argument of {cls.__name__}.substitute() or .safe_substitute() must be a mapping')

    @classmethod
    def _apply(cls, mapping: dict, key):
        if key is None:
            return mapping
        elif isinstance(key, _DefaultDict):
            return {k: key[k](v) for k, v in mapping.items()}
        elif isinstance(key, Mapping):
            return {k: key[k](v) if k in key else v for k, v in mapping.items()}
        elif callable(key):
            return {k: key(v) for k, v in mapping.items()}
        else:
            raise NotImplementedError(f"{cls.__name__}._apply_key(mapping, key=None) encountered invalid type for 'key'")


class _DefaultDict(dict):
    def __init__(self, dct, default=None):
        super().__init__(dct)
        self.default = default

    def __missing__(self, key):
        return self.default


def as_temp(path: Path):
    x = path.as_posix().split('.')
    x[1] = 'temp'
    return Path('.'.join(x))


class TempOpen:
    def __init__(self, path, mode='w', key=None, **kwargs):
        if 'w' not in mode:
            raise ValueError('TempOpen only works for writing files.')

        self.path = path if isinstance(path, Path) else Path(path)
        self.mode = mode
        self.kwargs = kwargs

        self.temp = as_temp(self.path)
        self.key = open if key is None else key
        self.file = None

    def __enter__(self):
        self.file = self.key(self.temp, mode=self.mode, **self.kwargs)
        return self.file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.__exit__(exc_type, exc_val, exc_tb)
        if (exc_type is None) and (exc_val is None) and (exc_tb is None):
            self.temp.replace(self.path)
        else:
            print(f'Due to error occurred while writing "{self.path}", it remains as temp file "{self.temp}"')
