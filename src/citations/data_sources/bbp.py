"""Tools for extracting info from BBP publication csv files."""

import logging
from typing import List, Optional

import pandas as pd

from citations.utils import normalize_title

logger = logging.getLogger(__name__)


def get_bbp_author_names(
    bbp_publications: pd.DataFrame, title: str, is_bbp: bool
) -> Optional[List[str]]:
    """
    Get names of BBP authors for a particular title.

    Parameters
    ----------
    bbp_publications : pd.DataFrame
        The DataFrame that contains the BBP publications.
    title : str
        The title of the publication.
    is_bbp : bool
        A flag indicating whether the publication is from BBP.

    Returns
    -------
    Optional[List[str]]
        A list of author names if the publication is from BBP, else None.
    """
    try:
        if is_bbp:
            bbp_row = bbp_publications[
                bbp_publications["normalized_title"] == normalize_title(title)
            ].iloc[0]
            author_names = [
                name.strip() for name in bbp_row["Author"].split(";")
            ]
        else:
            author_names = None
    except Exception as e:
        logger.error(f"Could not find bbp publication with title: {title}")
        raise e
    return author_names
