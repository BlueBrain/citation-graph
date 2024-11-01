"""Utility functions."""

import hashlib
import logging
import os
import re
import string
import time
from datetime import date, datetime
from typing import Any
from xml.etree import ElementTree as ET

import httpx
import pandas as pd
from httpx import HTTPError, HTTPStatusError, RequestError, Response

logger = logging.getLogger(__name__)


def get_with_waiting(
    endpoint: str, retry_times: int = 5, wait: float = 30
) -> Response:
    """
    Attempt to send a GET request to the specified endpoint with retries and waiting period.

    Parameters
    ----------
    endpoint : str
        The URL of the endpoint to send the GET request to.
    retry_times : int | None
        The number of times to retry the request in case of failure (default is 5).
    wait : float | None
        The waiting period (in seconds) between retries (default is 30).

    Returns
    -------
    Response
        The HTTP response received from the server.

    Raises
    ------
    RequestError
        If all retry attempts fail, the last caught RequestError is raised.
    """
    for i in range(retry_times):
        try:
            response = httpx.get(endpoint)
            response.raise_for_status()
            return response
        except (RequestError, HTTPError, HTTPStatusError) as e:
            # If we get an exception due to too many calls, wait and try again
            if i == retry_times - 1:
                raise e
            time.sleep(wait)
    raise Exception("Maximum retries reached")


def generate_unique_id(name: str) -> str:
    r"""Generate a semi-unique id based on a (institution) name.

    \f
    Parameters
    ----------
    name : str
        Any kind of arbitrary name.

    Returns
    -------
    str
        Generated sha256 id.
    """
    org_name_bytes = name.encode("utf-8")
    hash_object = hashlib.sha256(org_name_bytes)
    hash_hex = hash_object.hexdigest()
    return hash_hex[:8]


def normalize_title(text: str) -> str:
    r"""Normalize a title string.

    \f
    Parameters
    ----------
    text : str
        Title string to be normalized.

    Returns
    -------
    str
        Normalized title string, truncated to a maximum of 30 characters.
    """
    # Replace non-alphabetic and non-space characters with nothing
    text = re.sub(r"[^a-zA-Z\s]", "", text)
    # Replace multiple spaces with nothing
    text = re.sub(r"\s+", "", text)
    # Convert to lowercase
    text = text.lower()
    # Strip leading and trailing whitespace and punctuation
    text = text.strip(string.punctuation + string.whitespace)
    return text[:30].strip(string.punctuation + string.whitespace)


def is_valid_doi(doi_str):
    """Check if str is a valid DOI."""
    return bool(DOI_PATTERN.match(doi_str))


DOI_PATTERN = re.compile(r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$", re.IGNORECASE)


def to_date(date_str: str | Any) -> date | None:
    """
    Convert a date string to a datetime object.

    Parameters
    ----------
    date_str : str
        The date string to be converted.

    Returns
    -------
    datetime
        The converted datetime object.

    Raises
    ------
    ValueError
        If the date string has an invalid format.

    """
    try:
        if pd.isna(date_str):
            return None
        if len(date_str) == 4 and date_str.isdigit():
            return datetime(int(date_str), 1, 1)
        else:
            return pd.to_datetime(date_str).date()
    except Exception as e:
        raise ValueError(f"Invalid date format: {date_str}") from e


def load_europmc_xmls(europmc_article_xmls_path: str):
    """
    Load EuroPMC xml files.

    Parameters
    ----------
    europmc_article_xmls_path : str
        The path to the directory containing European PubMed Central (Europe PMC) XML files.

    Returns
    -------
    xml_map : dict
    """
    xml_map = {}
    for filename in os.listdir(europmc_article_xmls_path):
        if filename.endswith(".xml"):
            file_path = os.path.join(europmc_article_xmls_path, filename)
            tree = ET.parse(file_path)
            root = tree.getroot()
            file_key = os.path.splitext(filename)[0]
            xml_map[file_key] = root
    return xml_map


def save_xml_map(xml_map: dict, directory: str):
    """
    Save xmls fetched from EuroPMC.

    Parameters
    ----------
    xml_map : dict
        A dictionary representing the XML map, where the key is the name of the XML file and the value is the root element of the XML tree.

    directory : str
        The directory where the XML files will be saved.

    """
    for key, root in xml_map.items():
        filename = f"{key}.xml"
        file_path = os.path.join(directory, filename)

        if os.path.exists(file_path):
            base, ext = os.path.splitext(filename)
            counter = 1
            while os.path.exists(file_path):
                new_filename = f"{base}_{counter}{ext}"
                file_path = os.path.join(directory, new_filename)
                counter += 1

        tree = ET.ElementTree(root)
        tree.write(file_path, encoding="utf-8", xml_declaration=True)
