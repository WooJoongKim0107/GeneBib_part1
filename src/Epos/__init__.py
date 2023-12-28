__all__ = ['Epo', 'START', 'STOP']
from .containers import Epo
START = 0
STOP = 290

STARTS = [3*i for i in range(70)] + [3*70 + 2*i for i in range(38)] + [3*70+2*38 + 1*i for i in range(4)]
STOPS = [3*i for i in range(1, 71)] + [3*70 + 2*i for i in range(1, 39)] + [3*70+2*38 + 1*i for i in range(1, 5)]
