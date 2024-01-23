def get_title(title_localized):
    if title_localized.get('language', '') == 'en':
        return title_localized.get('text', '')
    else:
        return ''


def get_abstract(abstract_localized):
    if abstract_localized.get('language', '') == 'en':
        return abstract_localized.get('text', '')
    else:
        return ''


def parse_date(date):
    if date == '0':
        return {}
    else:
        year, month, date = date[:4], date[4:6], date[6:]
        return {'Year': int(year), 'Month': int(month), 'Date': int(date)}


def collect_cpcs(cpcs):
    return set(cpcs)


def collect_citations(citations):
    res = set()
    for citation in citations:
        match citation:
            case {'publication_number': '', 'application_number': ''}:
                pass
            case {'publication_number': '', 'application_number': str(x)}:
                res.add(f'app: {x}')
            case {'publication_number': str(x), 'application_number': ''}:
                res.add(f'pub: {x}')
    return res


def parse_us_patent(x):
    pub_number, app_number = x['publication_number'], x['application_number']
    title = get_title(x['title_localized'])
    abstract = get_abstract(x['abstract_localized'])

    filing_date = parse_date(x['filing_date'])
    pub_date = parse_date(x['publication_date'])
    grant_date = parse_date(x['grant_date'])

    cpcs = collect_cpcs(x['cpc'])
    citations = collect_citations(x['citation'])
    return pub_number, app_number, title, abstract, filing_date, pub_date, grant_date, cpcs, citations
