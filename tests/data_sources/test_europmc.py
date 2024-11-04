import xml.etree.ElementTree as ET
from unittest.mock import Mock, patch

import httpx
import pytest
from httpx import HTTPStatusError

from citations.data_sources.europmc import (
    extract_authors,
    extract_bbp_article,
    fetch_article_element,
    fetch_citation_ids,
    get_article,
    get_citations,
)
from citations.schemas import Article
from citations.utils import to_date


def generate_citation_response_xml(
    article_ids: list[str], source: str, hit_count: int
) -> str:
    """
    Generate an XML string for a given list of IDs.

    Parameters
    ----------
    article_ids : list of str
        List of IDs to include in the XML.
    source: str
        EuroPMC source
    hit_count: int
        Total number of results

    Returns
    -------
    str
        The generated XML as a string.
    """
    response_wrapper = ET.Element(
        "responseWrapper",
        {
            "xmlns:slx": "http://www.scholix.org",
            "xmlns:epmc": "https://www.europepmc.org/data",
        },
    )
    ET.SubElement(response_wrapper, "hitCount").text = str(hit_count)
    citation_list = ET.SubElement(response_wrapper, "citationList")
    for article_id in article_ids:
        citation = ET.SubElement(citation_list, "citation")
        ET.SubElement(citation, "id").text = article_id
        ET.SubElement(citation, "source").text = source

    return ET.tostring(response_wrapper, encoding="unicode")


def generate_single_result_xml(
    article_id,
    source,
    pmid,
    doi,
    title,
    isbn,
    pub_date=None,
    result_list=None,
    author_list=None,
):
    if result_list is None:
        result = ET.Element(
            "result",
            {
                "xmlns:slx": "http://www.scholix.org",
                "xmlns:epmc": "https://www.europepmc.org/data",
            },
        )
    else:
        result = ET.SubElement(result_list, "result")
    ET.SubElement(result, "id").text = article_id
    ET.SubElement(result, "source").text = source
    ET.SubElement(result, "pmid").text = pmid
    ET.SubElement(result, "doi").text = doi
    ET.SubElement(result, "title").text = title
    ET.SubElement(result, "isbn").text = isbn
    ET.SubElement(result, "firstPublicationDate").text = pub_date
    if author_list:
        authors = ET.SubElement(result, "authorIdList")
        for orcid_id in author_list:
            ET.SubElement(
                authors, "authorId", attrib={"type": "ORCID"}
            ).text = orcid_id
    return result


def generate_article_search_results_xml(
    article_ids, sources, pmids, dois, titles, isbns
):
    response_wrapper = ET.Element(
        "responseWrapper",
        {
            "xmlns:slx": "http://www.scholix.org",
            "xmlns:epmc": "https://www.europepmc.org/data",
        },
    )

    result_list = ET.SubElement(response_wrapper, "resultList")
    for i in range(len(article_ids)):
        generate_single_result_xml(
            article_ids[i],
            sources[i],
            pmids[i],
            dois[i],
            titles[i],
            isbns[i],
            result_list=result_list,
        )

    return ET.tostring(response_wrapper, encoding="unicode")


def test_fetch_citation_ids():
    response1 = httpx.Response(
        status_code=200,
        text=generate_citation_response_xml(["1", "2"], "MED", 6),
    )
    response2 = httpx.Response(
        status_code=200,
        text=generate_citation_response_xml(["3", "4"], "MED", 6),
    )
    response3 = httpx.Response(
        status_code=200,
        text=generate_citation_response_xml(["5", "6"], "MED", 6),
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        side_effect=[response1, response2, response3],
    ):
        citation_ids = fetch_citation_ids(
            "example_id", "example_source", page_size=2
        )

    assert citation_ids == ["1", "2", "3", "4", "5", "6"]


def test_fetch_citation_ids_self_citation():
    response1 = httpx.Response(
        status_code=200,
        text=generate_citation_response_xml(["1", "2"], "MED", 2),
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        side_effect=[response1],
    ):
        citation_ids = fetch_citation_ids("1", "example_source", page_size=2)

    assert citation_ids == ["2"]


def test_fetch_citation_ids_invalid_id():
    response = httpx.Response(
        status_code=200,
        text=(
            '<responseWrapper xmlns:slx="http://www.scholix.org"'
            ' xmlns:epmc="https://www.europepmc.org/data">'
            "<script/>"
            "<version>6.9</version>"
            "<hitCount>0</hitCount>"
            "<request>"
            "<id>000</id>"
            "<source>MED</source>"
            "<offSet>0</offSet>"
            "<pageSize>1000</pageSize>"
            "</request>"
            "<citationList/>"
            "</responseWrapper>"
        ),
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        side_effect=[response],
    ):
        citation_ids = fetch_citation_ids("example_id", "MED")
    assert len(citation_ids) == 0


def test_fetch_citation_ids_invalid_source():
    response = httpx.Response(
        status_code=404,
        text=(
            "<errorBean>"
            "<script/>"
            "<errCode>404</errCode>"
            "<errMsg>Invalid source provided. Please enter a valid source:"
            '"AGR","CBA","CIT","CTX","ETH","HIR","MED","PAT","PMC","PPR","NBK"</errMsg>'
            "</errorBean>"
        ),
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        side_effect=[response],
    ) as get_with_waiting:
        requests = Mock()
        get_with_waiting.side_effect = HTTPStatusError(
            "Invalid source provided. Please enter a valid source.",
            request=requests,
            response=response,
        )
        with pytest.raises(HTTPStatusError):
            fetch_citation_ids("example_id", "bad_source")


def test_fetch_article_element_doi():
    article_id = "article1"
    source = "MED"
    pmid = "1"
    doi1 = "doi1"
    title = "title1"
    isbn = "isbn1"
    response = httpx.Response(
        status_code=200,
        text=generate_article_search_results_xml(
            [article_id], [source], [pmid], [doi1], [title], [isbn]
        ),
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        return_value=response,
    ):
        article_element = fetch_article_element(doi1, "bad isbn", title)
        assert article_element.find("./id").text == article_id
        assert article_element.find("./source").text == source
        assert article_element.find("./pmid").text == pmid
        assert article_element.find("./title").text == title


def test_fetch_article_element_isbns():
    article_id = "article1"
    source = "MED"
    pmid = "pmid1"
    doi1 = "doi1"
    title = "title1"
    isbn = "isbn2"
    response1 = httpx.Response(
        status_code=200,
        text=generate_article_search_results_xml([], [], [], [], [], []),
    )
    response2 = httpx.Response(
        status_code=200,
        text=generate_article_search_results_xml([], [], [], [], [], []),
    )
    response3 = httpx.Response(
        status_code=200,
        text=generate_article_search_results_xml(
            [article_id], [source], [pmid], [doi1], [title], [isbn]
        ),
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        side_effect=[response1, response2, response3],
    ):
        article_element = fetch_article_element(
            "bad doi", "isbn1 isbn2", title
        )
        assert article_element.find("./id").text == article_id
        assert article_element.find("./source").text == source
        assert article_element.find("./pmid").text == pmid
        assert article_element.find("./title").text == title
        assert article_element.find("./isbn").text == isbn


def test_fetch_article_element_title():
    article_id = "article1"
    source = "MED"
    pmid = "1"
    doi1 = "doi1"
    title = "title1"
    isbn = "isbn1"
    response = httpx.Response(
        status_code=200,
        text=generate_article_search_results_xml(
            [article_id], [source], [pmid], [doi1], [title], [isbn]
        ),
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        return_value=response,
    ):
        article_element = fetch_article_element("baddoi", "bad isbn", title)
        assert article_element.find("./id").text == article_id
        assert article_element.find("./source").text == source
        assert article_element.find("./pmid").text == pmid
        assert article_element.find("./title").text == title


def test_fetch_article_element_not_found():
    response = httpx.Response(
        status_code=200,
        text=generate_article_search_results_xml([], [], [], [], [], []),
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        return_value=response,
    ):
        article_element = fetch_article_element("baddoi", "bad isbn", "title1")
        assert article_element is None


def test_extract_bbp_article():
    article_id = "article1"
    pmid = "1"
    doi1 = "doi1"
    title = "title1"
    isbn = "isbn1"
    pub_date = "2020-01-01"

    result = generate_single_result_xml(
        article_id, "europmc", pmid, doi1, title, isbn, pub_date
    )
    article, _, _ = extract_bbp_article(result, title, isbns=isbn)
    assert article.uid == article_id
    assert article.pmid == pmid
    assert article.doi == doi1
    assert article.title == title
    assert article.isbns == isbn
    expected_date = to_date(pub_date)
    assert article.publication_date == expected_date


def generate_get_article_response_xml(
    doi: str | None = None,
    title: str | None = None,
    abstract: str | None = None,
    urls: list[str] | None = None,
    pmid: str | None = None,
    europmc_id: str | None = None,
    date: str | None = None,
) -> str:
    response_wrapper = ET.Element(
        "responseWrapper",
        {},
    )
    result = ET.SubElement(response_wrapper, "resultList")
    if doi is None:
        return ET.tostring(response_wrapper, encoding="unicode")
    result = ET.SubElement(result, "result")
    ET.SubElement(result, "doi").text = doi
    ET.SubElement(result, "title").text = title
    ET.SubElement(result, "abstractText").text = abstract
    urls_elem = ET.SubElement(result, "fullTextUrlList")
    for url_text in urls:
        url = ET.SubElement(urls_elem, "fullTextUrl")
        url = ET.SubElement(url, "url")
        url.text = url_text
    ET.SubElement(result, "pmid").text = pmid
    ET.SubElement(result, "id").text = europmc_id
    ET.SubElement(result, "firstPublicationDate").text = date

    return ET.tostring(response_wrapper, encoding="unicode")


@pytest.mark.parametrize(
    "doi,title,abstract,urls,pmid,europmc_id,date",
    [
        (
            "doi1",
            "Great title",
            "Great abstract",
            ["url1", "url2"],
            "1",
            "id1",
            "2020-01-01",
        ),
        (
            "doi1",
            "Great title",
            "Great abstract",
            [],
            "1",
            "id1",
            "2020-01-01",
        ),
    ],
)
def test_get_article(doi, title, abstract, urls, pmid, europmc_id, date):
    response = httpx.Response(
        status_code=200,
        text=generate_get_article_response_xml(
            doi, title, abstract, urls, pmid, europmc_id, date
        ),
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        return_value=response,
    ):
        article = get_article(europmc_id)
        assert article.doi == doi
        assert article.title == title
        assert article.abstract == abstract
        if len(urls) > 0:
            assert article.url == urls[0]
        else:
            assert article.url is None
        assert article.pmid == pmid
        assert article.uid == europmc_id
        assert article.europmc_id == europmc_id
        assert str(article.publication_date) == date


def test_get_article_no_article():
    response = httpx.Response(
        status_code=200, text=generate_get_article_response_xml()
    )
    with patch(
        "citations.data_sources.europmc.get_with_waiting",
        return_value=response,
    ):
        with pytest.raises(ValueError):
            get_article("example_id")


@pytest.mark.parametrize(
    "citation_ids, articles",
    [
        (
            ["uid1", "uid2"],
            [
                Article(
                    uid="uid1",
                    title="title1",
                    publication_date="2020-01-01",
                    source="europmc",
                    is_bbp=True,
                    is_published=True,
                ),
                Article(
                    uid="uid2",
                    title="title2",
                    publication_date="2020-01-02",
                    source="europmc",
                    is_bbp=False,
                    is_published=True,
                ),
            ],
        ),
        ([], None),
    ],
)
def test_get_citations(citation_ids, articles):
    with patch(
        "citations.data_sources.europmc.fetch_citation_ids",
        return_value=citation_ids,
    ):
        with patch(
            "citations.data_sources.europmc.get_article", side_effect=articles
        ):
            citations, citing_articles = get_citations("uid3", "MED")

            for citation, citing_article in zip(citations, citing_articles):
                assert citation.article_uid_source == citing_article.uid
                assert citation.article_uid_target == "uid3"


def test_extract_authors():
    article_id = "article1"
    pmid = "1"
    doi1 = "doi1"
    title = "title1"
    isbn = "isbn1"
    pub_date = "2020-01-01"
    orcid_ids = ["orcid1", "orcid2"]

    result = generate_single_result_xml(
        article_id,
        "europmc",
        pmid,
        doi1,
        title,
        isbn,
        pub_date,
        author_list=orcid_ids,
    )
    assert extract_authors(result) == orcid_ids
