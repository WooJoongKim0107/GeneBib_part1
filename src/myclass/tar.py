import pickle
import tarfile
from pathlib import Path


class TarWrite:
    def __init__(self, name, mode='w'):
        self.name = name
        self.mode = mode
        self.tar_file: None | tarfile.TarFile = None
        self.paths = []

    def add(self, path: Path):
        self.tar_file.add(path, arcname=path.name)
        self.paths.append(path)

    def dump(self, obj, path: Path, mode='wb'):
        assert isinstance(path, Path), 'TarWrite.dump(path) only takes pathlib.Path object'
        with open(path, mode=mode) as f:
            pickle.dump(obj, f)
        self.add(path)

    def unlink_originals(self):
        for path in self.paths:
            path.unlink()

    def __enter__(self):
        self.tar_file = tarfile.open(self.name, mode=self.mode)
        return self

    def __exit__(self, typ, value, trace_back):
        self.tar_file.close()
        self.tar_file = None
        if (typ is None) and (value is None) and (trace_back is None):
            self.unlink_originals()


if __name__ == '__main__':
    from mypathlib import PathTemplate
    from multiprocessing import Pool

    # Usage 1st: With single CPU
    pt = PathTemplate('./${n}.pkl')
    with TarWrite('TarWriteTest.tar', 'w') as q:
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
        paths = p.map(main, range(5, 10))

    with TarWrite('TarWriteMpTest.tar', 'w') as q:
        for path in paths:
            q.add(path)

    # Usage 3rd: With multiprocessing.Pool.imap
    with TarWrite('TarWriteMpTest.tar', 'a') as q:
        with Pool(5) as p:
            for path in p.imap(main, range(11, 20)):
                q.add(path)
