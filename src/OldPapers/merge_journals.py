from .containers import Journal
from collections import Counter
from Papers.merge_journals import find_links, directly_linked, find_clusters, merge_all_equivalent_journals


def merge(cache=None):
    cache = cache if cache else Journal._CACHE
    clusters = find_clusters(directly_linked(find_links(cache)))
    counter = Counter(j for x in clusters.values() for j in x)
    assert sum(k for k, v in counter.items() if v > 1) == 0, 'Clustering failed'
    new_cache = merge_all_equivalent_journals(cache, clusters)
    assert len(set(new_cache.values())) == len(set(cache.values())) - len(counter) + len(clusters), 'Merging failed'
    return new_cache
