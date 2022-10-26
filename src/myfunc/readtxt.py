from typing import IO
from collections.abc import Iterable


Readable = Iterable[str] | IO[str]


def read_commented(x: Readable, mark='#'):
    lines = (line.partition(mark)[0].rstrip() for line in x)  # remove comments
    yield from (line for line in lines if line)  # remove empty lines
