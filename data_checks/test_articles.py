"""Tests for articles."""

import re

import pandas as pd
import pytest

from citations.dataframe import ARTICLE_COLUMN_DTYPES
from citations.utils import is_valid_doi


def test_unique_records(articles):
    """Ensure that each record is unique."""
    assert not articles.duplicated().any()


def test_columns(articles):
    """Ensure that columns are correct."""
    assert sorted(articles.columns) == sorted(ARTICLE_COLUMN_DTYPES.keys())


def test_uid_missing(articles):
    """Ensure that each record has an uid."""
    assert not articles["uid"].isna().any()


def test_uid_unique(articles):
    """Ensure that each id is unique."""
    assert len(articles["uid"].unique()) == len(articles)


def test_title_unique(articles):
    """Ensure that each title is unique."""
    pytest.skip(
        "We will have to normalize article titles before this will work."
    )
    assert len(articles["title"].unique()) == len(articles)


def test_publication_date_format(articles):
    """Ensure that date format is correct."""
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    def is_valid_date(date_str: str | None):
        return pd.isna(date_str) or bool(date_pattern.match(date_str))

    assert articles["publication_date"].apply(is_valid_date).all()


def test_is_bbp_type(articles):
    """Ensure is_bbp type."""
    assert articles["is_bbp"].dtype == bool


def test_is_bbp_missing(articles):
    """Ensure that is_bbp has no NaNs."""
    assert not articles["is_bbp"].isna().any()


def test_abstract_missing(articles):
    """Ensure that abstract has no NaNs."""
    pytest.skip("We don't have all the abstracts yet.")
    assert not articles["abstract"].isna().any()


def test_doi_format(articles):
    """Ensure that doi values are in the correct format."""
    dois = articles[~articles["doi"].isna()]["doi"]
    assert dois.apply(is_valid_doi).all()


def test_pmid_format(articles):
    """Ensure that pmid has correct format."""
    pmid_pattern = re.compile(r"^\d{8}$", re.IGNORECASE)

    def is_valid_pmid(pmid):
        return bool(pmid_pattern.match(str(pmid)))

    pmids = articles[~articles["pmid"].isna()]["pmid"]
    assert pmids.apply(is_valid_pmid).all()


def test_article_has_one_author(articles, author_wrote_article):
    """Ensure that each article has at least one author."""
    pytest.skip("We don't have all authors data for all articles yet.")
    assert (
        articles["uid"].isin(author_wrote_article["author_uid"].unique()).all()
    )
