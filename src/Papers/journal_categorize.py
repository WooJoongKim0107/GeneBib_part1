import pickle
import pandas as pd
from Papers import Journal  # Read
from mypathlib import PathTemplate


R_FILE0 = PathTemplate('$data/journal_curated_220523/wos-core_SCIE_2022-April-19_selected.csv').substitute()
R_FILE1 = PathTemplate('$data/journal_curated_220523/field2jnl_manually.pkl').substitute()
R_FILE2 = PathTemplate('$data/journal_curated_220523/jcr2genebib_categs.csv').substitute()
_R_FILE0 = PathTemplate('$pdata/paper/journal_cache.pkl.gz').substitute()
_R_FILE1 = PathTemplate('$lite/paper/jnls_selected.pkl').substitute()


