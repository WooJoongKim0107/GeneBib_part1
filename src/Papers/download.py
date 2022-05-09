from string import Template
from mypathlib import PathTemplate
from multiprocessing import Pool
from wget import download as wget_download


R_FILE = Template('https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed22n$number.xml.gz')
W_FILE = PathTemplate('$rsrc/data/paper/pubmed22n$number.xml.gz')


def download(n):
    url = R_FILE.substitute(number=f'{n:0>4}')
    path = W_FILE.substitute(number=f'{n:0>4}')
    if path.is_file():
        print(f'{n}: exist')
        pass
    else:
        print(n)
        wget_download(url, path.as_posix())


def main():
    with Pool(6) as p:
        p.map(download, list(range(1, 1115)))


if __name__ == '__main__':
    main()
