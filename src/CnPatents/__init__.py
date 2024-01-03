__all__ = ['CnPatent', 'START', 'STOP']
from .containers import CnPatent
START = 0
STOP = 3619

STARTS = [int(3619/112*i) for i in range(112)]
STOPS = [int(3619/112*(i+1)) for i in range(112)]
