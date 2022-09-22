import re
from lxml.etree import _Element as Element
from .containers import ISSN, choose_title


def parse_journal(pubmed_article_elt: Element):
    medline_ta: str = pubmed_article_elt.findtext('./MedlineCitation/MedlineJournalInfo/MedlineTA')
    nlm_unique_id: str | None = pubmed_article_elt.findtext('./MedlineCitation/MedlineJournalInfo/NlmUniqueID')
    issn: tuple[ISSN | None, str] = parse_issn(pubmed_article_elt.find('./MedlineCitation/Article/Journal/ISSN'))
    issn_l: tuple[ISSN | None, str] = _as_issn_l(pubmed_article_elt.findtext('./MedlineCitation/MedlineJournalInfo/ISSNLinking'))
    title: str | None = pubmed_article_elt.findtext('./MedlineCitation/Article/Journal/Title')
    iso_abbreviation: str | None = pubmed_article_elt.findtext('./MedlineCitation/Article/Journal/ISOAbbreviation')
    return medline_ta, nlm_unique_id, issn, issn_l, title, iso_abbreviation


def find_journal_key(pubmed_article_elt: Element):
    medline_ta: str = pubmed_article_elt.findtext('./MedlineCitation/MedlineJournalInfo/MedlineTA')
    iso_abbreviation: str | None = pubmed_article_elt.findtext('./MedlineCitation/Article/Journal/ISOAbbreviation')
    title: str | None = pubmed_article_elt.findtext('./MedlineCitation/Article/Journal/Title')
    key = choose_title(medline_ta, iso_abbreviation, title)
    return key


def parse_article(pubmed_article_elt: Element):
    pmid: int = int(pubmed_article_elt.findtext('./MedlineCitation/PMID'))
    pub_date: dict[str, str | int] = parse_pub_date(pubmed_article_elt.find('./MedlineCitation/Article/Journal/JournalIssue/PubDate'))
    title: str = pubmed_article_elt.findtext('./MedlineCitation/Article/ArticleTitle')
    abstract: str = merge_abstract_texts(pubmed_article_elt.findall('./MedlineCitation/Article/Abstract/AbstractText'))
    return pmid, pub_date, title, abstract


def parse_issn(issn_elt: Element|None):
    if issn_elt is None:
        return None, ''
    return ISSN(issn_elt.text), issn_elt.get('IssnType')


def _as_issn_l(x: str|None):
    if x is None:
        return None, ''
    return ISSN(x), 'Linking'


# MedlineDate formats:
# 1. '1998 Dec-1999 Jan'
# 2. '1997-1998'
# 3. '2010-2011 ' + str(Season|Month)
# 4. '1990'    <- I don't understand why they exist. It contradicts 190101.dtd.
# 5. 'Spring 2009'
# 6. '2003 ' + str
md_parser = re.compile(r'\b[12]\d{3}\b')  # r'(19|20)\d{2}(?=\s*)' on previous study


def parse_pub_date(pub_date_elt: Element):
    pub_date = children_as_dict(pub_date_elt)
    match pub_date:
        # PubDate element exist
        case {'Year': year, 'Month': month, 'Day': day}:
            return {'Year': int(year), 'Month': parse_month(month), 'Day': int(day)}
        case {'Year': year, 'Month': month}:
            return {'Year': int(year), 'Month': parse_month(month)}
        case {'Year': year, 'Season': season}:
            return {'Year': int(year), 'Season': season}
        case {'Year': year}:
            return {'Year': int(year)}

        # MedlineData exist
        case {'MedlineDate': valid_string} if (year:=md_parser.search(valid_string)):
            return {'Year': int(year.group())}
        case {'MedlineDate': _}:
            return pub_date

        # None of them exist: never happens
        case _:
            raise ValueError(f'Cannot parse {pub_date}')


_month = {s: i for i, s in enumerate(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], start=1)}
_month.update({s: i for i, s in enumerate(['January', 'February', 'March', 'April', 'May', 'June', 'July',
                                           'August', 'September', 'October', 'November', 'December'], start=1)})


def parse_month(x):
    if x in _month:
        return _month[x]
    else:
        return int(x)


def merge_abstract_texts(abstract_texts: list[Element]):
    lst = [txt if (txt := elt.text) else '' for elt in abstract_texts]
    return ' '.join(lst)


def children_as_dict(elt: Element):
    tags = [child.tag for child in elt]
    texts = [child.text for child in elt]
    return dict(zip(tags, texts))
