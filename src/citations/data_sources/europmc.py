"""Gather and process information from EuroPMC."""

import math
from datetime import date
from typing import Literal, Tuple
from urllib.parse import quote
from xml.etree.ElementTree import Element

from citations.data_sources.utils import parse_xml
from citations.schemas import Article, ArticleCitesArticle
from citations.utils import get_with_waiting, normalize_title, to_date


def get_citations(
    europmc_id: str,
    europmc_source: Literal[
        "AGR",
        "CBA",
        "CIT",
        "CTX",
        "ETH",
        "HIR",
        "MED",
        "PAT",
        "PMC",
        "PPR",
        "NBK",
    ],
    europmc_xml_map: dict | None = None,
) -> tuple[list[ArticleCitesArticle], list[Article]] | None:
    """
    Retrieve and process citations for a given article.

    Parameters
    ----------
    europmc_id : str
        The Europe PMC ID of the article to fetch citations for.
    europmc_source : str
        The source of the Euro PMC article.

    Returns
    -------
    tuple[list[ArticleCitesArticle], list[Article]]
        A list of ArticleCitesArticle objects representing the citations and cited articles as
        well as the list of citing articles.
    """
    citation_europmc_ids = fetch_citation_ids(europmc_id, europmc_source)
    if citation_europmc_ids is None:
        return None

    citing_articles = []
    citations = []
    for citation_europmc_id in citation_europmc_ids:
        article = get_article(citation_europmc_id, europmc_xml_map)
        if article is None:
            continue
        citing_articles.append(article)
        citations.append(
            ArticleCitesArticle(
                article_uid_source=citation_europmc_id,
                article_uid_target=europmc_id,
            )
        )
    return citations, citing_articles


def get_article(
    europmc_id: str, europmc_xml_map: dict | None = None
) -> Article | None:
    r"""Process a citation by fetching its metadata from Euro PMC.

    \f
    Parameters
    ----------
    europmc_id : str
        id for an EuroPMC article

    Returns
    -------
    Article
    """
    response = get_with_waiting(
        "https://www.ebi.ac.uk/europepmc/webservices"
        f"/rest/search?query=EXT_ID:{europmc_id}&resultType=core"
    )
    root = parse_xml(response.text)
    if root is None:
        return None

    if europmc_xml_map:
        europmc_xml_map[europmc_id] = root
    result = root.find("./resultList/result")
    if result is None:
        raise ValueError(f"Invalid europmc id: {europmc_id}")

    element = result.find("./doi")
    if element is not None:
        doi = element.text
    else:
        doi = None
    element = result.find("./title")
    if element is not None:
        title = element.text  # type: ignore
    element = result.find("./abstractText")
    if element is not None:
        abstract = element.text
    else:
        abstract = None
    element = result.find("./fullTextUrlList/fullTextUrl/url")
    if element is not None:
        url = element.text
    else:
        url = None
    element = result.find("./pmid")
    if element is not None and element.text is not None:
        pmid = str(int(element.text))
    else:
        pmid = None
    element = result.find("./firstPublicationDate")
    if element is not None and element.text is not None:
        publication_date = to_date(element.text)
    else:
        publication_date = None
    element = result.find("./citedByCount")
    if element is not None and element.text is not None:
        citations = int(element.text)
    else:
        citations = None

    return Article(
        uid=europmc_id,
        title=title,  # type: ignore
        publication_date=publication_date,  # type: ignore
        source="europmc",
        is_bbp=False,
        is_published=True,
        abstract=abstract,
        doi=doi,
        pmid=pmid,
        europmc_id=europmc_id,
        url=url,
        citations=citations,
    )


def extract_bbp_article(
    element: Element,
    title: str,
    abstract: str | None = None,
    doi: str | None = None,
    publication_date: date | None = None,
    url: str | None = None,
    isbns: str | None = None,
) -> Tuple[Article, str, str]:
    """
    Extract and construct a BBP article object from the given XML element.

    Parameters
    ----------
    element : Element
        The XML element containing article data.
    title : str
        The title of the article.
    abstract : str | None
        The abstract of the article, if available.
    doi : str | None
        The DOI of the article, if available.
    publication_date : date | None
        The publication date of the article, if available.
    url : str | None
        The URL of the article, if available.
    isbns : str | None
        The ISBNs of the article separated by spaces, if available.

    Returns
    -------
    Tuple[Article, str, str]
        A tuple containing the Article object, the source of the article from Europe PMC, and the Europe PMC ID.
    """
    pmid_elem = element.find("./pmid")
    if pmid_elem is not None and pmid_elem.text is not None:
        pmid = str(int(pmid_elem.text))
    else:
        pmid = None
    europmc_id_elem = element.find("./id")
    europmc_id = europmc_id_elem.text  # type: ignore
    if publication_date is None:
        publication_date_elem = element.find("./firstPublicationDate")
        if (
            publication_date_elem is not None
            and publication_date_elem.text is not None
        ):
            publication_date = to_date(publication_date_elem.text)
    europmc_source_elem = element.find("./source")  # type: ignore
    europmc_source = europmc_source_elem.text  # type: ignore
    if abstract is None:
        europmc_abstract = element.find("./abstractText")
        if europmc_abstract is not None:
            abstract = europmc_abstract.text
    if doi is None:
        europmc_doi = element.find("./doi")
        if europmc_doi is not None:
            doi = europmc_doi.text
    if url is None:
        europmc_url = element.find("./fullTextUrlList/fullTextUrl/url")
        if europmc_url is not None:
            url = europmc_url.text
    citations = element.find("./citedByCount")
    if citations is not None and citations.text is not None:
        citations_num = int(citations.text)
    else:
        citations_num = None

    article = Article(
        uid=europmc_id,  # type: ignore
        title=title,
        publication_date=publication_date,  # type: ignore
        source="europmc",
        is_bbp=True,
        is_published=True,
        abstract=abstract,
        doi=doi,
        pmid=pmid,
        europmc_id=europmc_id,
        url=url,
        isbns=isbns,
        citations=citations_num,
    )
    return article, europmc_source, europmc_id  # type: ignore


def extract_authors(element: Element) -> list[str]:
    r"""Get author orcid ids from the article if available.

    \f
    Parameters
    ----------
    element : Element
        The XML element containing article data.

    Returns
    -------
    list[str]
        A list of citation IDs associated with the given article.
    """
    # Maybe later we might want to add other author id types.
    elements = element.findall("./authorIdList/authorId[@type='ORCID']")
    return [
        orcid_id.text
        for orcid_id in elements
        if orcid_id is not None and orcid_id.text is not None
    ]


def fetch_citation_ids(
    europmc_id: str, europmc_source: str, page_size: int = 1000
) -> list[str] | None:
    r"""Fetch citation IDs for a given article from Europe PMC.

    \f
    Parameters
    ----------
    europmc_id: str
        The id for an article.
    europmc_source : str
        A source where europmc gets a certain article from.
    page_size: int
        EuroPMC page size (maximum is 1000).

    Returns
    -------
    list
        A list of citation IDs associated with the given article.
    """
    response = get_with_waiting(
        f"https://www.ebi.ac.uk/europepmc/webservices/rest/{europmc_source}/{europmc_id}"
        f"/citations?page=1&pageSize={page_size}&format=xml"
    )
    root = parse_xml(response.text)
    if root is None:
        return None

    num_citations = int(root.find(".//hitCount").text)  # type: ignore
    if num_citations == 0:
        return []
    pages = math.ceil(num_citations / page_size)
    citation_ids = [
        result.text for result in root.findall("./citationList/citation/id")
    ]
    for page in range(2, pages + 1):
        response = get_with_waiting(
            f"https://www.ebi.ac.uk/europepmc/webservices/rest/{europmc_source}/{europmc_id}"
            f"/citations?page={page}&pageSize={page_size}&format=xml"
        )
        root = parse_xml(response.text)
        if root is None:
            continue
        citation_ids.extend(
            [
                result.text
                for result in root.findall("./citationList/citation/id")
            ]
        )
    citation_ids = [
        citation_id
        for citation_id in citation_ids
        if citation_id != europmc_id
    ]
    return citation_ids  # type: ignore


def fetch_article_element(
    doi: str, isbns: str | None, title: str
) -> Element | None:
    """
    Retrieve the XML element of an article by its DOI, isbn or title.

    Parameters
    ----------
    doi : str | None
        The DOI of the article to search for.
    isbns : str | None
        The ISBNs of the article to search for separated by spaces.
    title : str
        The title of the article to search for if the DOI is not found.

    Returns
    -------
    Element | None
        The XML element of the article if found, otherwise None.
    """
    normalized_title = normalize_title(title)
    if doi is not None:
        response = get_with_waiting(
            "https://www.ebi.ac.uk/europepmc/webservices/rest/"
            f"search?query=DOI:{doi}&resultType=core"
        )
        root = parse_xml(response.text)
        if root is not None:
            article_element = root.find("./resultList/result")

            if article_element is not None:
                result_title = article_element.find("./title").text  # type: ignore
                if normalize_title(result_title) == normalized_title:  # type: ignore
                    return article_element

    if isbns is not None:
        for isbn in isbns.split():
            response = get_with_waiting(
                "https://www.ebi.ac.uk/europepmc/webservices/rest/"
                f"search?query=ISBN:{isbn}&resultType=core"
            )
            root = parse_xml(response.text)
            if root is not None:
                article_element = root.find("./resultList/result")
                if article_element is not None:
                    result_title = article_element.find("./title").text  # type: ignore
                    if normalize_title(str(result_title)) == normalized_title:  # type: ignore
                        return article_element

    # When using a title in a query we need to replace spaces
    query_title = quote(title)
    response = get_with_waiting(
        "https://www.ebi.ac.uk/europepmc/webservices"
        f"/rest/search?query={query_title}&resultType=core"
    )
    root = parse_xml(response.text)
    if root is None:
        return None
    results = root.findall("./resultList/result")
    for element in results:
        result_title = element.find("./title").text  # type: ignore
        if normalize_title(result_title) == normalized_title:  # type: ignore
            return element
    return None
