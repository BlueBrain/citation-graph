import xml.etree.ElementTree as ET
from datetime import date
from unittest.mock import patch

import httpx
import pytest

from citations.data_sources.orcid import (
    NAMESPACES,
    extract_affiliation_date,
    fetch_article_orcidids,
    filter_orcidids,
    get_author_orcid_information,
)


def generate_affiliation_xml(organization, start_date, end_date, pos_type="education"):
    xml_string = f"""
    <{pos_type}:{pos_type}-summary xmlns:{pos_type}="http://www.orcid.org/ns/{pos_type}"
        xmlns:common="http://www.orcid.org/ns/common">
        <common:start-date>
            <common:year>{str(start_date.year)}</common:year>
            <common:month>{str(start_date.month)}</common:month>
            <common:day>{str(start_date.day)}</common:day>
        </common:start-date>
        <common:end-date>
            <common:year>{str(end_date.year)}</common:year>
            <common:month>{str(end_date.month)}</common:month>
            <common:day>{str(end_date.day)}</common:day>
        </common:end-date>
        <common:organization>
            <common:name>{organization}</common:name>
            <common:address>
                <common:city>Ås</common:city>
                <common:country>NO</common:country>
            </common:address>
        </common:organization>
    </{pos_type}:{pos_type}-summary>
    """
    return ET.fromstring(xml_string)


@pytest.mark.parametrize(
    "date_type",
    ["start-date", "end-date"],
)
def test_extract_affiliation_date(date_type):
    organization = "org1"
    start_date = date(2020, 1, 1)
    end_date = date(2021, 1, 1)
    element = generate_affiliation_xml(organization, start_date, end_date, pos_type="education")

    if date_type == "start-date":
        expected_dt = start_date
    else:
        expected_dt = end_date

    # Parse the XML string
    dt = extract_affiliation_date(element, date_type)
    assert dt == expected_dt

    element = generate_affiliation_xml(organization, start_date, end_date, pos_type="work")

    if date_type == "start-date":
        expected_dt = start_date
    else:
        expected_dt = end_date

    # Parse the XML string
    dt = extract_affiliation_date(element, date_type)
    assert dt == expected_dt


def generate_orcid_list(orcid_ids: list[str]):
    response_wrapper = ET.Element(
        "search:search",
        {
            "xmlns:search": NAMESPACES["search"],
            "xmlns:common": NAMESPACES["common"],
        },
    )
    for orcidid in orcid_ids:
        result = ET.SubElement(response_wrapper, "search:result")
        orcid_identifier = ET.SubElement(result, "common:orcid-identifier")
        ET.SubElement(orcid_identifier, "common:path").text = orcidid
    return ET.tostring(response_wrapper, encoding="unicode")


def generate_author_record(titles: list[str], article_organizations=None, element=False):
    response_wrapper = ET.Element(
        "record:record",
        {
            "xmlns:record": NAMESPACES["record"],
            "xmlns:activities": NAMESPACES["activities"],
            "xmlns:work": NAMESPACES["work"],
            "xmlns:common": NAMESPACES["common"],
        },
    )
    activities = ET.SubElement(response_wrapper, "activities:activities-summary")
    works = ET.SubElement(activities, "activities:works")
    group = ET.SubElement(works, "activities:group")
    for i, title in enumerate(titles):
        work_summary = ET.SubElement(group, "work:work-summary")
        work_title = ET.SubElement(work_summary, "work:title")
        ET.SubElement(work_title, "common:title").text = title
        if article_organizations is not None:
            org = article_organizations[i]
            org_name = org["name"]
            org_id = org["id"]
            org_source = org["source"]
            organization = ET.SubElement(work_summary, "common:organization")
            ET.SubElement(organization, "common:name").text = org_name
            dis_org = ET.SubElement(organization, "disambiguated-organization")
            ET.SubElement(dis_org, "common:disambiguated-organization").text = org_id
            ET.SubElement(dis_org, "common:disambiguated-organization-identifier").text = org_source

    if element:
        return response_wrapper
    else:
        return ET.tostring(response_wrapper, encoding="unicode")


def test_filter_orcidids():
    orcid_ids = ["orcid1", "orcid2"]
    article_title = "In Silico Brain Imaging"
    bad_orcid_id = "orcid1"
    good_orcid_id = "orcid2"
    response1 = httpx.Response(
        status_code=200,
        text=generate_author_record(["From Big Data to Big Displays", "A Physically Plausible Model"]),
    )
    response2 = httpx.Response(
        status_code=200,
        text=generate_author_record(["In Silico Brain Imaging", "Large Volume Imaging of Rodent Brain"]),
    )
    with patch(
        "citations.data_sources.orcid.get_with_waiting",
        side_effect=[response1, response2],
    ):
        filtered_orcidids = filter_orcidids(orcid_ids, article_title)
        assert bad_orcid_id not in filtered_orcidids
        assert good_orcid_id in filtered_orcidids


def test_fetch_article_authors_doi():
    orcid_ids = ["orcid1", "orcid2"]
    response = httpx.Response(status_code=200, text=generate_orcid_list(orcid_ids))
    with patch(
        "citations.data_sources.orcid.get_with_waiting",
        return_value=response,
    ):
        with patch(
            "citations.data_sources.orcid.filter_orcidids",
            side_effect=lambda article_orcid_ids, article_title: article_orcid_ids,
        ):
            authors = fetch_article_orcidids("doi1", "1", "title1")
            assert sorted(authors) == sorted(orcid_ids)


def test_fetch_article_authors_pmid():
    orcid_ids = ["orcid1", "orcid2"]
    response1 = httpx.Response(status_code=400, text="")
    response2 = httpx.Response(status_code=200, text=generate_orcid_list(orcid_ids))
    with patch(
        "citations.data_sources.orcid.get_with_waiting",
        side_effect=[response1, response2],
    ):
        with patch(
            "citations.data_sources.orcid.filter_orcidids",
            side_effect=lambda article_orcid_ids, article_title: article_orcid_ids,
        ):
            from citations.data_sources.orcid import fetch_article_orcidids

            authors = fetch_article_orcidids("wrong doi", "1", "title1")
            assert sorted(authors) == sorted(orcid_ids)


def test_get_author_affiliation():
    orcidid = "orcid1"
    titles = ["title1", "title2"]
    expected_institutions = [
        {"name": "org1_name", "id": "org1_id", "source": "ROR"},
        {"name": "org2_name", "id": "org2_id", "source": "GRID"},
    ]
    record = generate_author_record(titles, expected_institutions, element=True)
    from citations.data_sources.orcid import get_author_affiliations

    institutions, affiliations = get_author_affiliations(orcidid, record)
    for i in range(len(institutions)):
        inst = institutions[i]
        aff = affiliations[i]
        expected_inst = expected_institutions[i]
        assert inst.name == expected_inst["name"]
        assert inst.uid == expected_inst["id"]
        assert inst.organization_id_source == expected_inst["source"]
        assert aff.author_uid == orcidid
        assert aff.institution_uid == expected_inst["id"]


def test_get_author_information():
    author_uid = "author1"
    orcidid = "orcid1"
    google_scholar_id = "scholar1"
    bbp_author_name = "name1"
    article_uid = "article1"

    response = httpx.Response(status_code=200, text="<fake>fake</fake>")
    with patch(
        "citations.data_sources.orcid.get_with_waiting",
        return_value=response,
    ):
        with patch(
            "citations.data_sources.orcid.get_author_name",
            return_value="Guy Manson",
        ):
            with patch(
                "citations.data_sources.orcid.get_author_affiliations",
                return_value=(None, None),
            ):
                (
                    author,
                    author_wrote_article,
                    _,
                    _,
                ) = get_author_orcid_information(
                    author_uid,
                    orcidid,
                    google_scholar_id,
                    bbp_author_name,
                    article_uid,
                )

                assert author.uid == author_uid
                assert author_wrote_article.author_uid == author_uid
                assert author_wrote_article.article_uid == article_uid
