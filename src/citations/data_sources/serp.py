"""Tools for extracting author information from Google Scholar using Serp."""

import json
import os

import pandas as pd
from pandas import DataFrame

from citations.utils import normalize_title


def create_author_id_mapping(directory: str):
    """
    Map author names to the json result we get from Serp api.

    Parameters
    ----------
    directory : str
        The directory path where JSON files are located.

    Returns
    -------
    dict
        A dictionary mapping the file keys to their corresponding author IDs.

    """
    author_id_mapping = {}

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "r") as file:
                data = json.load(file)
                profiles = data.get("profiles", [])
                if len(profiles) == 1:
                    author_id = profiles[0].get("author_id")
                    file_key = os.path.splitext(filename)[0]
                    author_id_mapping[file_key] = author_id

    return author_id_mapping


def get_all_bbp_publications(
    bbp_publications_path: str,
    bbp_articles_wip_path: str,
    bbp_theses_wip_path: str,
) -> DataFrame:
    """Get all BBP publications.

    Parameters
    ----------
    bbp_publications_path : str
        Path to the file containing BBP publications data.

    bbp_articles_wip_path : str
        Path to the file containing BBP articles work-in-progress data.

    bbp_theses_wip_path : str
        Path to the file containing BBP theses work-in-progress data.

    Returns
    -------
    DataFrame
        A DataFrame containing all BBP publications, including articles and theses work-in-progress.

    """
    bbp_publications = pd.read_csv(bbp_publications_path)
    bbp_wip_articles = pd.read_csv(bbp_articles_wip_path)
    bbp_wip_articles["is_published"] = False
    bbp_publications = pd.concat([bbp_publications, bbp_wip_articles])
    bbp_wip_theses = pd.read_csv(bbp_theses_wip_path)
    bbp_wip_theses["is_published"] = False
    bbp_publications = pd.concat([bbp_publications, bbp_wip_theses])
    bbp_publications["normalized_title"] = bbp_publications["Title"].apply(
        lambda title: normalize_title(title)
    )
    return bbp_publications
