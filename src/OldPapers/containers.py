from mypathlib import PathTemplate
from Papers.containers import Journal


Journal._CACHE = {}
Journal._CACHE_PATH = PathTemplate('$rsrc/pdata/pubmed20n_gz/journal_cache.pkl.gz').substitute()
if Journal._CACHE_PATH.is_file():
    Journal.import_cache()
