import pickle
import tarfile
from collections.abc import Iterable
from pathlib import Path


class TarWrite:
    def __init__(self, name, mode='w'):
        self.name = name
        self.mode = mode
        self.tar_file: None | tarfile.TarFile = None
        self.paths = []

    def extend(self, paths: Iterable[Path]):
        for path in paths:
            self.add(path)

    def add(self, path: Path):
        self.tar_file.add(path, arcname=path.name)
        self.paths.append(path)

    def dump(self, obj, path: Path):
        assert isinstance(path, Path), 'TarWrite.dump(path) only takes pathlib.Path object'
        with open(path, mode='wb') as f:
            pickle.dump(obj, f)
        self.add(path)

    def unlink(self):
        for path in self.paths:
            path.unlink()
        self.paths.clear()

    def close(self):
        self.tar_file.close()
        self.tar_file = None

    def __enter__(self):
        self.tar_file = tarfile.open(self.name, mode=self.mode)
        return self

    def __exit__(self, typ, value, trace_back):
        self.close()
        if (typ is None) and (value is None) and (trace_back is None):
            self.unlink()


class TarRead:
    def __init__(self, name, mode='r'):
        self.name = name
        self.mode = mode
        self.tar_file: None | tarfile.TarFile = None
        self.files = []

    def extractfile(self, name):
        member = self.tar_file.getmember(name)
        f = self.tar_file.extractfile(member)
        self.files.append(f)
        return f

    def load(self, name):
        f = self.extractfile(name)
        return pickle.load(f)

    def close(self):
        self.tar_file.close()
        self.tar_file = None
        for file in self.files:
            file.close()
        self.files.clear()

    def __enter__(self):
        self.tar_file = tarfile.open(self.name, mode=self.mode)
        return self

    def __exit__(self, typ, value, trace_back):
        self.close()


class TarRW(TarRead, TarWrite):
    def __init__(self, name, mode='r'):
        super().__init__(name, mode)
        self.paths = []

    def __exit__(self, typ, value, trace_back):
        self.close()
        if (typ is None) and (value is None) and (trace_back is None):
            self.unlink()


if __name__ == '__main__':
    from mypathlib import PathTemplate
    from multiprocessing import Pool

    # Usage 1st: With single CPU
    pt = PathTemplate('./${n}.pkl')
    with TarRW('TarWriteTest.tar', 'w') as q:
        for n in range(10):
            q.dump(list(range(n)), pt.substitute(n=n))

    # Usage 2nd: With multiprocessing.Pool.map
    pt = PathTemplate('./${n}_mp.pkl')

    def main(x):
        res = list(range(x))
        with open(pt.substitute(n=x), 'wb') as file:
            pickle.dump(res, file)
        return pt.substitute(n=x)

    with Pool(5) as p:
        paths_ = p.map(main, range(5, 10))

    with TarRW('TarWriteMpTest.tar', 'w') as q:
        q.extend(paths_)

    # Usage 3rd: With multiprocessing.Pool.imap
    with TarRW('TarWriteMpTest.tar', 'a') as q:
        with Pool(5) as p:
            for path in p.imap(main, range(11, 20)):
                q.add(path)
