from mypathlib import PathTemplate
from UniProt.containers import Nested


_R_FILE = PathTemplate('$rsrc/pdata/uniprot/uniprot_keywords.pkl').substitute()
_W_FILE = PathTemplate('$rsrc/pdata/uniprot/nested.pkl')


def main():
    q = Nested(load=False)
    q.dump()


if __name__ == '__main__':
    main()
