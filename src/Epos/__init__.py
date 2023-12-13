__all__ = ['Epo', 'START', 'STOP']
from .containers import Epo
START = 0
STOP = 220
STARTS = [i for i in range(112-1)] + [112-1]
STOPS = [i+1 for i in range(112-1)] + [STOP]
