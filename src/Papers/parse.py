import re
from xml.etree.ElementTree import Element
from Papers.containers import ISSN, issn_factory, ISSNl


def parse_journal(pubmed_article_elt: Element):
    medline_ta: str = pubmed_article_elt.findtext('./MedlineCitation/MedlineJournalInfo/MedlineTA')
    nlm_unique_id: str|None = pubmed_article_elt.findtext('./MedlineCitation/MedlineJournalInfo/NlmUniqueID')
    issn: ISSN|None = parse_issn(pubmed_article_elt.find('./MedlineCitation/Article/Journal/ISSN'))
    issn_l: ISSNl|None = _as_issn_l(pubmed_article_elt.findtext('./MedlineCitation/MedlineJournalInfo/ISSNLinking'))
    title: str|None = pubmed_article_elt.findtext('./MedlineCitation/Article/Journal/Title')
    iso_abbreviation: str|None = pubmed_article_elt.findtext('./MedlineCitation/Article/Journal/ISOAbbreviation')
    return medline_ta, nlm_unique_id, issn, issn_l, title, iso_abbreviation


def parse_article(pubmed_article_elt: Element):
    pmid: int = int(pubmed_article_elt.findtext('./MedlineCitation/PMID'))
    pub_date: dict[str, str|int] = parse_pub_date(pubmed_article_elt.find('./MedlineCitation/Article/Journal/JournalIssue/PubDate'))
    title: str = pubmed_article_elt.findtext('./MedlineCitation/Article/ArticleTitle')
    abstract: str = merge_abstract_texts(pubmed_article_elt.findall('./MedlineCitation/Article/Abstract/AbstractText'))
    return pmid, pub_date, title, abstract


def parse_issn(issn_elt: Element|None):
    if issn_elt is None:
        return None
    issn_val, issn_type = issn_elt.text, issn_elt.get('IssnType')
    return issn_factory(issn_val, issn_type)


def _as_issn_l(x: str|None):
    if x:
        return ISSNl(x)
    else:
        return None


_medline_date_parser = re.compile('[12]\d{3}(?=\s)')
def parse_pub_date(pub_date_elt: Element):
    pub_date = children_as_dict(pub_date_elt)
    match pub_date:
        # PubDate element exist
        case {'Year': year, 'Month': month, 'Day': day}:
            return {'Year': int(year), 'Month': month, 'Day': int(day)}
        case {'Year': year, 'Month': month}:
            return {'Year': int(year), 'Month': month}
        case {'Year': year, 'Season': season}:
            return {'Year': int(year), 'Season': season}
        case {'Year': year}:
            return {'Year': int(year)}

        # MedlineData exist
        case {'MedlineDate': valid_string} if (year:=_medline_date_parser.match(valid_string)):
            return {'MedlineDate': int(year.group())}
        case {'MedlineDate': _}:
            return pub_date

        # None of them exist: never happens
        case _:
            raise ValueError(f'Cannot parse {pub_date}')


def merge_abstract_texts(abstract_texts: list[Element]):
    lst = [txt if (txt:=elt.text) else '' for elt in abstract_texts]
    return ' '.join(lst)


def children_as_dict(elt: Element):
    tags = [child.tag for child in elt]
    texts = [child.text for child in elt]
    return dict(zip(tags, texts))
