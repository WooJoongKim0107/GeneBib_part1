import os
import psutil
from contextlib import contextmanager


def check_memory_pct():
    memory_usage_dict = dict(psutil.virtual_memory()._asdict())
    memory_usage_percent = memory_usage_dict['percent']
    return memory_usage_percent


def check_memory_abs():
    pid = os.getpid()
    current_process = psutil.Process(pid)
    current_process_memory_usage_as_MB = current_process.memory_info().rss / 1e6
    return current_process_memory_usage_as_MB


@contextmanager
def eyes_on():
    print(f"Enter: memory_usage_percent: {check_memory_pct()}%")
    print(f"Enter: Current memory MB   : {check_memory_abs():.3f} MB")
    yield
    print(f" Exit: memory_usage_percent: {check_memory_pct()}%")
    print(f" Exit: Current memory MB   : {check_memory_abs():.3f} MB")
