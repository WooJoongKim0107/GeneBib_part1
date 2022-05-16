import os
import psutil
from contextlib import contextmanager
from string import Template
from textwrap import dedent


def check_memory_pct():
    memory_usage_dict = dict(psutil.virtual_memory()._asdict())
    memory_usage_percent = memory_usage_dict['percent']
    return memory_usage_percent


def check_memory_abs():
    pid = os.getpid()
    current_process = psutil.Process(pid)
    current_process_memory_usage_as_MB = current_process.memory_info().rss / 1e6
    return current_process_memory_usage_as_MB


_SUM_TEMP = Template(dedent("""\
            Enter:
                memory_usage_percent: ${old_pct} %
                Current memory MB   : ${old_abs} MB
             Exit:
                memory_usage_percent: ${new_pct} %
                Current memory MB   : ${new_abs} MB
          Changes:
                ${del_pct} %
                ${del_abs} MB"""))

@contextmanager
def eyes_on(change_only=True):
    old_pct = check_memory_pct()
    old_abs = check_memory_abs()
    yield
    new_pct = check_memory_pct()
    new_abs = check_memory_abs()
    del_pct = new_pct - old_pct
    del_abs = new_abs - old_abs
    dct = {k: f'{v:.2f}' for k, v in locals().items()}
    summary = _SUM_TEMP.substitute(dct)
    if change_only:
        print('\n'.join(summary.splitlines()[-3:]))
    else:
        print(summary)
