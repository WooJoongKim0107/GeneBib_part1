import json
from mypathlib import PathTemplate
from Communities import C2E


_R_FILE = PathTemplate('$data/community/update_curated_cmnt_map_231201.pkl').substitute()
W_FILE = PathTemplate('$pdata/to_part2/clusters.json').substitute()


def main():
    c2e = C2E.load()  # sorted by its key
    with open(W_FILE, 'w') as file:
        json.dump(list(c2e.values()), file)
    return None


if __name__ == '__main__':
    main()
