from mypathlib import PathTemplate
from UniProt.containers import Nested


_R_FILE = PathTemplate('$rsrc/pdata/uniprot/uniprot_keywords.pkl').substitute()
_W_FILE = PathTemplate('$rsrc/pdata/uniprot/nested.pkl')


main = Nested.build


if __name__ == '__main__':
    main()
