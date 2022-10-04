def parse(x: str):
    sep = '\n\n'
    for v in x.split(sep):
        yield parse_paragraph(v)


def parse_paragraph(x: str):
    cmnt_line, *rest = x.splitlines()
    cmnt_idx = int(cmnt_line.split('cmnt No.')[1].strip())

    entries = []
    added_keywords = []
    removed_keywords = []
    for line in rest:
        line: str = line.strip()
        if line.startswith('+ '):
            added_keywords.append(line)
        elif line.startswith('- '):
            removed_keywords.append(line)
        else:
            entries.append(line)
    return cmnt_idx, entries, added_keywords, removed_keywords
