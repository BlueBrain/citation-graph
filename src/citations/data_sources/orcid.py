"""Orcid-related functions and constants."""

import calendar
import logging
from datetime import date
from typing import Literal, cast
from urllib.parse import quote
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element

import httpx
from httpx import HTTPStatusError

import citations.data_sources.utils
from citations.schemas import (
    Author,
    AuthorAffiliatedWithInstitution,
    AuthorWroteArticle,
    Institution,
    OrganizationIdSource,
)
from citations.utils import generate_unique_id, get_with_waiting, normalize_title

logger = logging.getLogger(__name__)


NAMESPACES = {
    "activities": "http://www.orcid.org/ns/activities",
    "common": "http://www.orcid.org/ns/common",
    "distinction": "http://www.orcid.org/ns/distinction",
    "employment": "http://www.orcid.org/ns/employment",
    "education": "http://www.orcid.org/ns/education",
    "funding": "http://www.orcid.org/ns/funding",
    "invited-position": "http://www.orcid.org/ns/invited-position",
    "personal-details": "http://www.orcid.org/ns/personal-details",
    "person": "http://www.orcid.org/ns/person",
    "record": "http://www.orcid.org/ns/record",
    "search": "http://www.orcid.org/ns/search",
    "membership": "http://www.orcid.org/ns/membership",
    "peer-review": "http://www.orcid.org/ns/peer-review",
    "qualification": "http://www.orcid.org/ns/qualification",
    "research-resource": "http://www.orcid.org/ns/research-resource",
    "service": "http://www.orcid.org/ns/service",
    "work": "http://www.orcid.org/ns/work",
}

AFFILIATION_TYPES = [
    "distinction",
    "education",
    "employment",
    "funding",
    "invited-position",
    "membership",
    "peer-review",
    "qualification",
    "research-resource",
    "service",
    "work",
]


def extract_affiliation_date(position: Element, date_type: Literal["start-date", "end-date"]) -> date | None:
    """
    Extract a date from the given XML element based on the specified date type.

    Parameters
    ----------
    position : Element
        The XML element from which to extract the date.
    date_type : Literal["start-date", "end-date"]
        The type of date to extract ("start-date" or "end-date").

    Returns
    -------
    date | None
        The extracted date, or None if the date is not available or incomplete.
    """
    element = position.find(
        f"./common:{date_type}/common:year",
        namespaces=NAMESPACES,
    )
    year_str = None if element is None else element.text
    element = position.find(
        f"./common:{date_type}/common:month",
        namespaces=NAMESPACES,
    )
    month_str = None if element is None else element.text
    element = position.find(
        f"./common:{date_type}/common:day",
        namespaces=NAMESPACES,
    )
    day_str = None if element is None else element.text
    if year_str is not None:
        year = int(year_str)
        if month_str is not None and day_str is not None:
            month = int(month_str)
            day = int(day_str)
            try:
                dt = date(year, month, day)
            except ValueError:
                # This error happens when they accidentally give an out of range value for day
                _, last_day = calendar.monthrange(year, month)
                dt = date(year, int(month), last_day)
        else:
            dt = date(year, 1, 1)
    else:
        dt = None
    return dt


def fetch_article_orcidids(doi: str | None, pmid: str | None, article_title: str, top_n_orcid: int = 5) -> list[str]:
    """
    Fetch all orcid ids for authors of this article.

    Parameters
    ----------
    doi : str | None
        DOI identifier for article if available
    pmid : str | None
        PMID identifier for article if available
    article_title : str
        Title of this article.

    Returns
    -------
    list | None
        List of authors for this article if they can be found in Orcid.
    """
    article_orcid_ids = []
    if doi is not None:
        endpoint = f"https://pub.orcid.org/v3.0/search/?q=doi-self:{doi}"
        article_orcid_ids.extend(get_orcidids_from_endpoint(endpoint)[:top_n_orcid])

    if pmid is not None:
        endpoint = f"https://pub.orcid.org/v3.0/search/?q=pmid:{pmid}"
        article_orcid_ids.extend(get_orcidids_from_endpoint(endpoint)[:top_n_orcid])

    article_orcid_ids = list(set(article_orcid_ids))
    article_orcid_ids = filter_orcidids(article_orcid_ids, article_title)
    return list(set(article_orcid_ids))


def get_orcidids_from_author_names(author_names: list[str], max_author_names: int = 3) -> list[str]:
    """
    Fetch all orcid ids for authors of this article.

    Parameters
    ----------
    author_names : str | None
        DOI identifier for article if available

    Returns
    -------
    list[str]
        List of authors with these names if they can be found in Orcid.
    """
    orcid_ids = []
    for author_name in author_names:
        names = author_name.strip().split(",")
        family_name = names[0]
        given_names = " ".join(names[1:])
        family_name = family_name.strip()
        given_names = given_names.strip()
        try:
            endpoint = (
                f"https://pub.orcid.org/v3.0/search/?q=family-name:"
                f"{quote(family_name)}+AND+given-names:{quote(given_names)}"
            )
            response = get_with_waiting(endpoint)
        except HTTPStatusError as e:
            logger.error(f"Error sending request: {str(e)}")
            return []

        if response.status_code == 200:
            root = citations.data_sources.utils.parse_xml(response.text)
            if root is None:
                continue

            elements = root.findall(
                "./search:result/common:orcid-identifier/common:path",
                namespaces=NAMESPACES,
            )[:max_author_names]
            if len(elements) == 0:
                continue
            if len(elements) == 1:
                if elements[0].text is not None:
                    orcid_ids.append(elements[0].text)
            else:  # multiple potential authors
                found_exact_name = False
                logger.warning(f"More than one author found with endpoint {endpoint}")
                exact_name = f"{given_names} {family_name}"
                logger.warning(f"Looking for author with exact name {exact_name}")
                orcidids_with_exact_name = []
                for orcid_id_element in elements:
                    endpoint = f"https://pub.orcid.org/v3.0/{orcid_id_element.text}/record"
                    response = get_with_waiting(endpoint)

                    record = citations.data_sources.utils.parse_xml(response.text)
                    if record is None:
                        continue
                    name = get_author_name(record)
                    if name == f"{given_names} {family_name}":
                        if orcid_id_element.text is not None:
                            orcid_ids.append(orcid_id_element.text)
                        found_exact_name = True
                        break
                if not found_exact_name:
                    if elements[0].text is not None:
                        orcid_ids.append(elements[0].text)
                    try:
                        record = ET.fromstring(response.text)
                    except ET.ParseError as e:
                        logger.error(f"Error parsing result of call to {endpoint}")
                        logger.error(f"Response text: {response.text}")
                        raise e

                    name = get_author_name(record)
                    if name == f"{given_names} {family_name}":
                        if orcid_id_element.text is not None:
                            orcid_ids.append(orcid_id_element.text)
                        found_exact_name = True
                        break
                if not found_exact_name:
                    if elements[0].text is not None:
                        orcid_ids.append(elements[0].text)
                    if name == exact_name:
                        if orcid_id_element.text is not None:
                            orcidids_with_exact_name.append(orcid_id_element.text)
                if len(orcidids_with_exact_name) == 0:
                    logger.warning(f"Cannot find exact name '{exact_name}'.")
                    for i in range(len(elements)):
                        if elements[i].text is not None:
                            logger.warning(f"Choosing id: {elements[i].text}")
                            orcid_ids.append(elements[i].text)  # type: ignore
                elif len(orcidids_with_exact_name) > 1:
                    logger.warning(
                        "There is more than one id with exact name '{}'.".format(f"{given_names} {family_name}")
                    )
                    logger.warning(f"Choosing id: {orcidids_with_exact_name[0]}")
                    orcid_ids.append(orcidids_with_exact_name[0])
                else:
                    orcid_ids.append(orcidids_with_exact_name[0])

    return orcid_ids


def get_orcidids_from_endpoint(endpoint: str) -> list[str]:
    """
    Fetch all orcid ids using the given endpoint.

    Parameters
    ----------
    endpoint : str
        endpoint url to use for author searching.

    Returns
    -------
    list[str]
        List of author ids if they can be found in Orcid.
    """
    try:
        response = get_with_waiting(endpoint)
    except httpx.HTTPStatusError:
        logger.warning(f"Could not fetch authors for endpoint: {endpoint}.")
        return []

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError:
        logger.error(f"Error parsing result of call to {endpoint}")
        logger.error(f"Response text: {response.text}")
        return []

    elements = root.findall(
        "./search:result/common:orcid-identifier/common:path",
        namespaces=NAMESPACES,
    )
    return [path.text for path in elements if path.text is not None]


def filter_orcidids(orcid_ids: list[str], article_title: str) -> list[str]:
    """
    Filter orcidids to the authors that are actually authors of the article.

    Parameters
    ----------
    orcid_ids : list[str]
        List of ids to filter
    article_title : str
        Title of the article.

    Returns
    -------
    list[str]
        List of authors for this article if they can be found in Orcid.
    """
    normalized_article_title = normalize_title(article_title)
    actual_author_orcidids = []
    for orcidid in orcid_ids:
        response = get_with_waiting(f"https://pub.orcid.org/v3.0/{orcidid}/record")

        root = citations.data_sources.utils.parse_xml(response.text)
        if root is None:
            continue

        elements = root.findall(
            "./activities:activities-summary/activities:*/activities:group/*/*/common:title",
            namespaces=NAMESPACES,
        )
        normalized_titles = {normalize_title(element.text) for element in elements if element.text is not None}
        if normalized_article_title in normalized_titles:
            actual_author_orcidids.append(orcidid)
    return actual_author_orcidids


def get_author_orcid_information(
    author_uid: str,
    orcidid: str,
    google_scholar_id: str | None,
    bbp_author_name: str | None,
    current_article_uid: str,
) -> (
    tuple[
        Author,
        AuthorWroteArticle,
        list[Institution],
        list[AuthorAffiliatedWithInstitution],
    ]
    | None
):
    r"""Process author affiliation data for a given article and author.

    \f
    Parameters
    ----------
    orcidid : str
        The ORCID ID of the author.
    current_article_uid: str
        Id of the article where this author was extracted from.

    Returns
    -------
    None
    """
    author_wrote_article = AuthorWroteArticle(author_uid=author_uid, article_uid=current_article_uid)
    endpoint = f"https://pub.orcid.org/v3.0/{orcidid}/record"
    response = get_with_waiting(endpoint)

    record = citations.data_sources.utils.parse_xml(response.text)
    if record is None:
        return None

    if bbp_author_name is None:
        name = get_author_name(record)
    else:
        name = bbp_author_name
    author = Author(
        uid=author_uid,
        orcid_id=orcidid,
        google_scholar_id=google_scholar_id,
        name=name,
    )
    institutions, affiliations = get_author_affiliations(orcidid, record)
    return author, author_wrote_article, institutions, affiliations


def get_author_name(record: Element) -> str | None:
    r"""Process and normalize author affiliation data for a given article and author.

    \f
    Parameters
    ----------
    record: : Element
        XML metadata of this authors information.

    Returns
    -------
    str | None
        Name of the author if available.
    """
    element = record.find(".//person:name", namespaces=NAMESPACES)
    if element is not None:
        given_names_element = element.find(".//personal-details:given-names", namespaces=NAMESPACES)
        family_name_element = element.find(".//personal-details:family-name", namespaces=NAMESPACES)
        if given_names_element is not None and family_name_element is not None:
            if given_names_element.text is not None and family_name_element.text is not None:
                name = given_names_element.text + " " + family_name_element.text
                return name
    return None


def get_author_affiliations(
    orcidid: str,
    record: Element,
) -> tuple[list[Institution], list[AuthorAffiliatedWithInstitution]]:
    """
    Process author affiliation data from an ORCID record.

    Parameters
    ----------
    orcidid : str
        The ORCID ID of the author whose affiliations are being processed.
    record : Element
        The XML element containing the ORCID record data.
    """
    positions = []
    for affiliation in AFFILIATION_TYPES:
        positions.extend(
            record.findall(
                f".//{affiliation}:{affiliation}-summary",
                namespaces=NAMESPACES,
            )
        )

    institutions = []
    affiliations = []
    for position in positions:
        element = position.find(
            ".//common:organization/common:name",
            namespaces=NAMESPACES,
        )
        if element is None or element.text is None:
            continue
        organization = element.text

        start_date = extract_affiliation_date(position, "start-date")
        end_date = extract_affiliation_date(position, "end-date")

        element = position.find(
            ".//common:disambiguated-organization-identifier",
            namespaces=NAMESPACES,
        )
        if element is not None and element.text is not None:
            org_id = element.text
        else:
            # If missing, what should be the placeholder id ?
            org_id = generate_unique_id(organization)

        element = position.find(
            ".//common:disambiguation-source",
            namespaces=NAMESPACES,
        )
        if element is not None and element.text is not None:
            org_source = cast(OrganizationIdSource, element.text)
        else:
            org_source = "sha256"

        institutions.append(
            Institution(
                uid=org_id,
                name=organization,
                organization_id=org_id,
                organization_id_source=org_source,
            )
        )
        affiliations.append(
            AuthorAffiliatedWithInstitution(
                author_uid=orcidid,
                institution_uid=org_id,
                start_date=start_date,
                end_date=end_date,
            )
        )

    return institutions, affiliations


def get_article_from_endpoint(endpoint: str) -> str | None:
    """
    Use endpoint to get article xml.

    Parameters
    ----------
    endpoint : str
        The endpoint URL from which to retrieve the article.

    Returns
    -------
    str | None
        The response text from the endpoint if the request was successful,
        or None if there was an error or if no elements were found.
    """
    try:
        response = get_with_waiting(endpoint)
    except httpx.HTTPStatusError:
        return None

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError:
        return None

    elements = root.findall(
        "./search:result/common:orcid-identifier/common:path",
        namespaces=NAMESPACES,
    )
    if len(elements) == 0:
        return None

    return response.text
