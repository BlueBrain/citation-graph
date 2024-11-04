"""Gather article and author records from Orcid."""

import argparse
import logging
import os
import pathlib
from glob import glob
from xml.etree import ElementTree as ET

import pandas as pd
from httpx import HTTPError, HTTPStatusError, RequestError
from pandas import DataFrame
from tqdm import tqdm

from citations.data_sources.orcid import NAMESPACES, get_article_from_endpoint, get_orcidids_from_author_names
from citations.utils import get_with_waiting, load_europmc_xmls

logger = logging.getLogger(__name__)


def get_parser() -> argparse.ArgumentParser:
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--articles_path",
        type=pathlib.Path,
        help="Path the the articles csv produced by 'gather_articles.py'",
        default="data/articles.csv",
    )
    parser.add_argument(
        "--bbp_articles_path",
        type=pathlib.Path,
        default=(
            "data/publication_data/bbp_publications/BBP official list of" " publications and theses 11jul2024.csv"
        ),
    )
    parser.add_argument(
        "--orcid_article_records_dir",
        type=pathlib.Path,
        help=("Directory where we save all article records which lists article" " authors."),
        default="data/publication_data/article_authors/orcid",
    )
    parser.add_argument(
        "--orcid_author_records_dir",
        type=pathlib.Path,
        help=("Directory where we save all author records searched by orcid id" " and author name."),
        default="data/publication_data/author_profiles/orcid",
    )
    parser.add_argument(
        "--europmc_article_xmls_path",
        type=pathlib.Path,
        help="Path to the directory where we save and load europmc xmls",
        default="data/publication_data/articles",
    )
    return parser


def fetch_authors_by_europmc_record(europmc_article_xmls_path: str, orcid_author_records_dir: str):
    """
    Extract orcidids from europmc records and fetch authors.

    Parameters
    ----------
    europmc_article_xmls_path : str
        The file path to the directory containing the XML files downloaded from Europe PMC.

    orcid_author_records_dir : str
        The file path to the directory where the fetched author records will be saved.

    """
    europmc_xml_map = load_europmc_xmls(europmc_article_xmls_path)

    for root in tqdm(europmc_xml_map.values()):
        orcidids = [
            elem.text
            for elem in root.findall(
                "./resultList/result/authorIdList/authorId[@type='ORCID']",
                namespaces=NAMESPACES,
            )
        ]
        for orcidid in orcidids:
            author_path = os.path.join(orcid_author_records_dir, f"{orcidid}.xml")
            if os.path.exists(author_path):
                continue
            endpoint = f"https://pub.orcid.org/v3.0/{orcidid}/record"
            try:
                response = get_with_waiting(endpoint)
            except (RequestError, HTTPError, HTTPStatusError):
                continue

            try:
                record = ET.fromstring(response.text)
                if record is None:
                    continue
            except ET.ParseError:
                continue
            with open(author_path, "w") as f:
                f.write(response.text)


def main(
    articles_path: str,
    bbp_articles_path: str,
    orcid_article_records_dir: str,
    orcid_author_records_dir: str,
    europmc_article_xmls_path: str,
) -> None:
    """
    Gather article and author records from Orcid.

    Parameters
    ----------
    articles_path : str
        The path to the CSV file containing articles information.
    bbp_articles_path : str
        The path to the directory where BBP articles should be stored.
    orcid_article_records_dir : str
        The path to the directory where ORCID article records should be stored.
    orcid_author_records_dir : str
        The path to the directory where ORCID author records should be stored.

    Returns
    -------
    None

    """
    articles = pd.read_csv(articles_path)
    fetch_article_records(articles, orcid_article_records_dir)  # use DOI/PMID in ORCID API to get WROTE edges
    fetch_authors_by_orcidid(orcid_article_records_dir, orcid_author_records_dir)  # author nodes
    fetch_authors_by_author_name(
        articles_path, bbp_articles_path, orcid_author_records_dir
    )  # WROTE edge from name without orcid id but still from europmc
    fetch_authors_by_europmc_record(
        europmc_article_xmls_path, orcid_author_records_dir
    )  # WROTE edges from orcid id obtained from europmc


def fetch_authors_by_author_name(articles_path: str, bbp_articles_path: str, orcid_author_records_dir: str) -> None:
    """
    Fetch authors for a BBP article using author names.

    Parameters
    ----------
    articles_path : str
        The file path of the CSV containing article information.

    bbp_articles_path : str
        The file path of the CSV containing BBP publication information.

    orcid_author_records_dir : str
        The directory path where ORCID author records will be saved.

    Returns
    -------
    None
        This method does not return anything.

    Raises
    ------
    None

    """
    articles = pd.read_csv(articles_path)
    bbp_publications = pd.read_csv(bbp_articles_path)
    bbp_publications = bbp_publications[bbp_publications["Title"].isin(articles["title"])]
    for _, bbp_row in tqdm(bbp_publications.iterrows()):
        uid = articles[articles["title"] == bbp_row["Title"]]["uid"].iloc[0]
        path = os.path.join(orcid_author_records_dir, f"{uid}.xml")
        if os.path.exists(path):
            continue
        author_names = bbp_row["Author"].split(";")

        article_orcid_ids = get_orcidids_from_author_names(author_names)
        if len(article_orcid_ids) == 0:
            continue

        root = ET.Element(
            "search:search",
            {
                "xmlns:search": NAMESPACES["search"],
                "xmlns:common": NAMESPACES["common"],
            },
        )
        for orcid_id in article_orcid_ids:  # lots of dan kellers
            result = ET.SubElement(root, "search:result")
            orcid_identifier = ET.SubElement(result, "common:orcid-identifier")
            element = ET.SubElement(orcid_identifier, "common:path")
            element.text = orcid_id
        tree = ET.ElementTree(root)
        tree.write(path)


def fetch_authors_by_orcidid(orcid_article_records_dir: str, orcid_author_records_dir: str):
    """
    Fetch authors for a BBP article using orcidid.

    Parameters
    ----------
    orcid_article_records_dir : str
        The directory path where the ORCID article records are stored.

    orcid_author_records_dir : str
        The directory path where the ORCID author records will be saved.

    Returns
    -------
    None

    """
    article_xmls = glob(os.path.join(orcid_article_records_dir, "*.xml"))
    for article_xml in tqdm(article_xmls):
        with open(article_xml) as f:
            content = f.read()

        root = ET.fromstring(content)

        elements = root.findall(
            "./search:result/common:orcid-identifier/common:path",
            namespaces=NAMESPACES,
        )
        orcidids = [path.text for path in elements if path.text is not None]
        for orcidid in orcidids:
            author_path = os.path.join(orcid_author_records_dir, f"{orcidid}.xml")
            if os.path.exists(author_path):
                continue
            endpoint = f"https://pub.orcid.org/v3.0/{orcidid}/record"
            response = get_with_waiting(endpoint)

            try:
                record = ET.fromstring(response.text)
                if record is None:
                    continue
            except ET.ParseError:
                continue
            with open(author_path, "w") as f:
                f.write(response.text)


def fetch_article_records(articles: DataFrame, orcid_article_records_dir: str) -> None:
    """
    Fetch xml records showing the authors of each article based on searching on Orcid.

    Parameters
    ----------
    articles : pandas DataFrame
        The DataFrame containing the article's information.

    orcid_article_records_dir : str
        The directory where the article records will be saved.

    Returns
    -------
    None

    """
    for _, row in tqdm(articles.iterrows(), desc="Fetching article records"):
        uid = row.uid
        article_path = os.path.join(orcid_article_records_dir, f"{uid}.xml")
        if os.path.exists(article_path):
            continue
        doi = row.doi if not pd.isna(row.doi) else None

        fetched = False
        if doi is not None:
            article_text = get_article_from_endpoint(f"https://pub.orcid.org/v3.0/search/?q=doi-self:{doi}")
            if article_text is not None:
                with open(article_path, "w") as f:
                    f.write(article_text)
                fetched = True

        pmid = row.pmid if not pd.isna(row.pmid) else None
        if not fetched and pmid is not None:
            article_text = get_article_from_endpoint(f"https://pub.orcid.org/v3.0/search/?q=pmid:{pmid}")
            if article_text is None:
                continue
            with open(article_path, "w") as f:
                f.write(article_text)


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    main(
        args.articles_path,
        args.bbp_articles_path,
        args.orcid_article_records_dir,
        args.orcid_author_records_dir,
        args.europmc_article_xmls_path,
    )
