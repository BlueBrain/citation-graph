"""Tests for authors."""

import re

from citations.dataframe import AUTHOR_COLUMNS


def test_unique_records(authors):
    """Ensure that each record is unique."""
    assert not authors.duplicated().any()


def test_columns(authors):
    """Ensure that columns are correct."""
    assert sorted(authors.columns) == sorted(AUTHOR_COLUMNS)


def test_uid_missing(authors):
    """Ensure that each record has an uid."""
    assert not authors["uid"].isna().any()


def test_uid_unique(authors):
    """Ensure that each uid is unique."""
    assert len(authors["uid"].unique()) == len(authors)


def test_orcid_id_format(authors):
    """Ensure that orcid_id has the correct format."""
    orcid_pattern = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[X\d]$")

    def is_valid_orcidid(orcid: str):
        return bool(orcid_pattern.match(orcid))

    assert authors["orcid_id"].apply(is_valid_orcidid).all()
