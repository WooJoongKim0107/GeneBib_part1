__all__ = ['EpPatent', 'START', 'STOP']
from .containers import EpPatent
START = 0
STOP = 439

STARTS = [int(439/112*i) for i in range(112)]
STOPS = [int(439/112*(i+1)) for i in range(112)]
