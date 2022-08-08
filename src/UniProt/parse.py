from xml.etree.ElementTree import Element
from UniProt.containers import UniProtEntry

header = '{http://uniprot.org/uniprot}'
len_header = len(header)


def parse_date(x):
    return {k: int(v) for k, v in zip(['Year', 'Month', 'Day'], x.split('-'))}


def hmm(entry_elt):
    targets = {'accession': [], 'name': [], 'protein': [], 'gene': [], 'reference': []}
    for elt in entry_elt:
        tag = elt.tag[len_header:]
        if tag in targets:
            targets[tag].append(elt)
    return targets


def well(entry_elt):
    hmmm = hmm(entry_elt)
    return dict(created=parse_date(entry_elt.get('created')),  # YYYY-MM-DD
                name=hmmm['name'][0].text,  # one and only one in ['name']
                accessions=[elt.text for elt in hmmm['accession']],
                proteins=[(tag, text) for elt in hmmm['protein'] for tag, text in xplore(elt)],
                genes=[(tag, text) for elt in hmmm['gene'] for tag, text in xplore_gene(elt)],
                refs=[parse_ref(elt) for elt in hmmm['reference']],)


def xplore(elt, *tags):
    tags = *tags, elt.tag[28:]
    if len(elt):
        for child in elt:
            yield from xplore(child, *tags)
    else:
        yield tags, elt.text


def xplore_gene(elt, *tags):
    tags = *tags, elt.tag[28:]
    if len(elt):
        for child in elt:
            yield from xplore_gene(child, *tags)
    else:
        yield (*tags, elt.attrib['type']), elt.text


def parse_ref(elt: Element):
    cit_elt = elt.find(f'{header}citation')
    for db_ref in cit_elt.findall(f'{header}dbReference'):
        if db_ref.get('type') == 'PubMed':
            pmid = int(db_ref.get('id'))
            break
    else:
        pmid = 0

    return dict(pub_date=parse_date(cit_elt.get('date')),  # YYYY / YYYY-MM / YYYY-MM-DD
                type=cit_elt.attrib.get('type', ''),
                # Counter({'submission': 171924,
                #          'journal article': 1099401,
                #          'thesis': 459,
                #          'book': 1858,
                #          'online journal article': 626,
                #          'unpublished observations': 510,
                #          'patent': 214})
                scopes=[elt.text if elt.text else '' for elt in elt.findall(f'{header}scope')],
                pmid=pmid,  # 9,762 citations (~0.88%) does not include PMID among 1,099,401 journal article citations
                title=cit_elt.findtext(f'{header}title'),)


if __name__ == '__main__':
    import gzip, pickle
    from xml.etree.ElementTree import parse

    with gzip.open('./uniprot_sprot.xml.gz', 'rb') as file:
        root = parse(file).getroot()[:-1]

    q = {}
    for elt in root:
        w = UniProtEntry(well(elt))
        q[w.key_acc] = w
    with gzip.open('./uniprot_sprot_parsed.pkl.gz', 'wb') as file:
        pickle.dump(q, file)

    _keywords = {k: 0 for x in q.values() for k in x.keywords}
    keywords = list(_keywords)
    with open('./uniprot_keywords.pkl', 'wb') as file:
        pickle.dump(keywords, file)

