"""Misc tools for extracting author information."""

import logging
import os
import xml
from typing import Dict, List
from xml.etree import ElementTree as ET

import pandas as pd
from pandas import DataFrame, Series

import citations.data_sources.orcid as orcid
from citations.dataframe import (
    AUTHOR_AFFILIATED_WITH_INSTITUTION_COLUMNS,
    AUTHOR_COLUMNS,
    AUTHOR_WROTE_ARTICLE_COLUMNS,
    INSTITUTION_COLUMNS,
)

logger = logging.getLogger(__name__)


def load_authors_state(checkpoint_dir: str, articles: pd.DataFrame, only_get_bbp_authors: bool) -> tuple:
    """
    Load a checkpoint for the gather authors script if exists or return an empty state.

    Parameters
    ----------
    checkpoint_dir : str
        The directory path where the checkpoint files are stored.
    articles : pd.DataFrame
        The DataFrame containing the articles data.
    only_get_bbp_authors : bool
        A flag indicating whether to only include authors who are part of the Broad Bioimage Benchmark collection.

    Returns
    -------
    tuple
        A tuple containing the following:
        - articles_processed : pd.DataFrame
            The DataFrame containing the processed articles data.
        - author_aff_institution : list
            A list of dictionaries representing author affiliation with institutions.
        - author_wrote_article : list
            A list of dictionaries representing author-article relationships.
        - authors : list
            A list of dictionaries representing authors.
        - institutions : list
            A list of dictionaries representing institutions.
        - remaining_articles : pd.DataFrame
            The DataFrame containing the remaining articles after processing.
        - all_author_uids : set
            A set of unique author UIDs.
        - all_institution_uids : set
            A set of unique institution UIDs.
    """
    all_author_uids = set()
    all_institution_uids = set()
    ckpt_exists = False
    if checkpoint_dir is not None:
        os.makedirs(checkpoint_dir, exist_ok=True)
        articles_processed_path = os.path.join(checkpoint_dir, "articles_processed.csv")
        ckpt_exists = os.path.exists(articles_processed_path)

    if ckpt_exists:
        articles_processed = pd.read_csv(
            articles_processed_path,
            dtype={
                "uid": str,
                "title": str,
                "publication_date": str,
                "source": str,
                "is_bbp": bool,
                "abstract": str,
                "doi": str,
                "pmid": str,
                "europmc_id": str,
                "url": str,
                "isbns": str,
            },
        )
        authors_df = pd.read_csv(os.path.join(checkpoint_dir, "authors.csv"))
        authors = [dict(author) for _, author in authors_df.iterrows()]
        all_author_uids.update([author["uid"] for author in authors])
        institutions_df = pd.read_csv(os.path.join(checkpoint_dir, "institutions.csv"))
        institutions = [dict(institution) for _, institution in institutions_df.iterrows()]
        all_institution_uids.update([inst["uid"] for inst in institutions])
        author_wrote_article_df = pd.read_csv(os.path.join(checkpoint_dir, "author_wrote_article.csv"))
        author_wrote_article = [dict(wrote) for _, wrote in author_wrote_article_df.iterrows()]
        author_aff_institution_df = pd.read_csv(
            os.path.join(
                checkpoint_dir,
                "author_affiliated_with_institution.csv",
            )
        )
        author_aff_institution = [dict(aff) for _, aff in author_aff_institution_df.iterrows()]

        # Subtract already processed articles
        merged_df = articles.merge(articles_processed, how="left", indicator=True)
        remaining_articles = merged_df[merged_df["_merge"] == "left_only"].drop(columns="_merge")
    else:
        articles_processed = pd.DataFrame(columns=articles.columns)
        authors = []
        institutions = []
        author_aff_institution = []
        author_wrote_article = []
        remaining_articles = articles

    if only_get_bbp_authors:
        remaining_articles = remaining_articles[remaining_articles["is_bbp"]]

    return (
        articles_processed,
        author_aff_institution,
        author_wrote_article,
        authors,
        institutions,
        remaining_articles,
        all_author_uids,
        all_institution_uids,
    )


def parse_xml(response_text: str) -> ET.Element | None:
    """
    Parse XML response text and return ElementTree object.

    Parameters
    ----------
    response_text : str
        The XML response text to be parsed.

    Returns
    -------
    Element | None
        The parsed XML element if successful, or None if there was an error parsing the XML.
    """
    try:
        return ET.fromstring(response_text)
    except xml.etree.ElementTree.ParseError:
        logger.error(f"Error parsing XML: {response_text}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return None


def save_authors_results(
    author_aff_institution: list[dict],
    author_wrote_article: list[dict],
    authors: list[dict],
    institutions: list[dict],
    output_dir: str,
):
    """Save results to a directory."""
    df = pd.DataFrame(
        author_aff_institution,
        columns=AUTHOR_AFFILIATED_WITH_INSTITUTION_COLUMNS,
    )
    if len(df) > 0:
        df.sort_values(by=["author_uid", "institution_uid"], inplace=True)
    df.to_csv(
        os.path.join(output_dir, "author_affiliated_with_institution.csv"),
        index=False,
        header=True,
    )
    df = pd.DataFrame(institutions, columns=INSTITUTION_COLUMNS)
    if len(df) > 0:
        df.sort_values(by="uid", inplace=True)
    df.to_csv(os.path.join(output_dir, "institutions.csv"), index=False, header=True)
    df = pd.DataFrame(authors, columns=AUTHOR_COLUMNS)
    if len(df) > 0:
        df.sort_values(by="uid", inplace=True)
    df.to_csv(os.path.join(output_dir, "authors.csv"), index=False, header=True)
    df = pd.DataFrame(author_wrote_article, columns=AUTHOR_WROTE_ARTICLE_COLUMNS)
    if len(df) > 0:
        df.sort_values(by=["author_uid", "article_uid"], inplace=True)
    df.to_csv(
        os.path.join(output_dir, "author_wrote_article.csv"),
        index=False,
        header=True,
    )


def get_author_ids(
    bbp_author_names: list[str] | None,
    doi: str,
    europmc_wrote: DataFrame,
    google_scholar_ids: Dict[str, str],
    pmid: str,
    row: Series,
    top_n_orcid: int = 5,
) -> List[Dict[str, str | None]]:
    """
    Get all possible and types of ids for authors.

    Parameters
    ----------
    bbp_author_names : Optional[list[str]
        A dictionary mapping author names to corresponding ORCID IDs for BBP authors.
    doi : str
        A string representing the DOI (Digital Object Identifier) of the article.
    europmc_wrote : DataFrame
        A DataFrame containing information about authors and their affiliations for the article.
    google_scholar_ids : Dict[str, str]
        A dictionary mapping author names to corresponding Google Scholar IDs.
    pmid : str
        A string representing the PMID (PubMed ID) of the article.
    row : Series
        A pandas Series object representing information about the article.

    Returns
    -------
    List[Dict[str, Optional[str]]]
        A list of dictionaries, where each dictionary represents an author and their associated information.

    """
    article_orcid_ids = list(europmc_wrote[europmc_wrote["article_uid"] == row.uid]["author_uid"])
    article_orcid_ids.extend(orcid.fetch_article_orcidids(doi, pmid, row.title, top_n_orcid=top_n_orcid))
    article_orcid_ids = sorted(set(article_orcid_ids))
    author_ids = [
        {
            "orcidid": orcid_id,
            "google_scholar_id": None,
            "author_name": None,
        }
        for orcid_id in article_orcid_ids
    ]
    if bbp_author_names is not None:
        set_author_ids(author_ids, bbp_author_names, google_scholar_ids)
    return author_ids


def save_checkpoint(
    author_aff_institution: list[dict],
    author_wrote_article: list[dict],
    authors: list[dict],
    institutions: list[dict],
    articles_processed: pd.DataFrame,
    checkpoint_dir: str,
) -> None:
    """
    Save the current state of the processing by writing the processed articles data and authors' results to a directory.

    Parameters
    ----------
    author_aff_institution : dict
        A dictionary mapping author names to their affiliations.
    author_wrote_article : dict
        A dictionary mapping author names to the articles they have written.
    authors : dict
        A dictionary mapping author names to their details.
    institutions : dict
        A dictionary mapping institution names to their details.
    articles_processed : pd.DataFrame
        A DataFrame containing the processed articles data.
    """
    articles_processed.to_csv(
        os.path.join(checkpoint_dir, "articles_processed.csv"),
        index=False,
        header=True,
    )
    save_authors_results(
        author_aff_institution,
        author_wrote_article,
        authors,
        institutions,
        checkpoint_dir,
    )


def set_author_ids(
    author_ids: list[dict[str, str | None]],
    author_names: list[str],
    google_scholar_ids: dict[str, str],
) -> None:
    """
    Set both orcid and Google Scholar ids for each author when available.

    Parameters
    ----------
    author_ids : list[dict[str, str]]
        The list of dictionaries containing author IDs.

    author_names : list[str]
        The list of author names.

    google_scholar_ids : dict[str, str]
        The dictionary mapping author names to their Google Scholar IDs.

    Returns
    -------
    None

    """
    for author_name in author_names:
        google_scholar_id = google_scholar_ids.get(author_name)
        orcid_ids = orcid.get_orcidids_from_author_names([author_name])

        if google_scholar_id is None and len(orcid_ids) == 0:
            author_ids.append(
                {
                    "orcidid": None,
                    "google_scholar_id": None,
                    "author_name": author_name,
                }
            )
            continue

        if len(orcid_ids) == 0:
            author_ids.append(
                {
                    "orcidid": None,
                    "google_scholar_id": google_scholar_id,
                    "author_name": author_name,
                }
            )
            continue

        orcid_id = orcid_ids[0]
        filtered_author_ids: list[dict[str, str | None]] = []
        for ids in filtered_author_ids:
            if ids["orcidid"] != orcid_ids:
                filtered_author_ids.append(ids)
        author_ids = filtered_author_ids
        author_ids.append(
            {
                "orcidid": orcid_id,
                "google_scholar_id": google_scholar_id,
                "author_name": author_name,
            }
        )
