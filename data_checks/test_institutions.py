"""Tests for institutions."""

import re

import pytest

from citations.dataframe import INSTITUTION_COLUMNS

RINGGOLD_PATTERN = re.compile(r"^\d+$")
LEI_PATTERN = re.compile(r"^[A-Z\d]{20}$")
GRID_PATTERN = re.compile(r"^grid\.\d+\.[a-z\d]+$")
FUNDREF_PATTERN = re.compile(r"^http://dx.doi.org/10.13039/\d+$")
ROR_PATTERN = re.compile(r"^https://ror.org/[a-z\d]+$")
SHA256_PATTERN = re.compile(r"^[a-z\d]{8}$")


def is_valid_ringgold(value):
    """Ensure that value has correct RINGGOLD format."""
    return bool(RINGGOLD_PATTERN.match(value))


def is_valid_lei(value):
    """Ensure that value has correct LEI format."""
    return bool(LEI_PATTERN.match(value))


def is_valid_grid(value):
    """Ensure that value has correct GRID format."""
    return bool(GRID_PATTERN.match(value))


def is_valid_fundref(value):
    """Ensure that value has correct FUNDREF format."""
    return bool(FUNDREF_PATTERN.match(value))


def is_valid_ror(value):
    """Ensure that value has correct ROR format."""
    return bool(ROR_PATTERN.match(value))


def is_valid_sha256(value):
    """Ensure that value has correct sha256 format."""
    return bool(SHA256_PATTERN.match(value))


def test_unique_records(institutions):
    """Ensure that each record is unique."""
    assert not institutions.duplicated().any()


def test_columns(institutions):
    """Ensure that institutions has correct columns."""
    assert (institutions.columns == INSTITUTION_COLUMNS).all()


def test_uid_missing(institutions):
    """Ensure that each record has an uid."""
    assert not institutions["uid"].isna().any()


def test_name_unique(institutions):
    """Ensure that each institution name only appears once."""
    pytest.skip(
        "Right now there are duplicate names. Eventually we want to"
        " deduplicate institutions"
    )
    assert len(institutions["name"].unique()) == len(institutions)


def test_organization_id_source(institutions):
    """Ensure that each institution has a correct id source."""
    assert (
        institutions["organization_id_source"]
        .isin(["sha256", "RINGGOLD", "LEI", "GRID", "FUNDREF", "ROR"])
        .all()
    )


def test_organization_id(institutions):
    """Ensure that each institution has a correct id."""
    ringgold = institutions[
        institutions["organization_id_source"] == "RINGGOLD"
    ]
    assert ringgold["organization_id"].apply(is_valid_ringgold).all()
    lei = institutions[institutions["organization_id_source"] == "LEI"]
    assert lei["organization_id"].apply(is_valid_lei).all()
    grid = institutions[institutions["organization_id_source"] == "GRID"]
    assert grid["organization_id"].apply(is_valid_grid).all()
    fundref = institutions[institutions["organization_id_source"] == "FUNDREF"]
    assert fundref["organization_id"].apply(is_valid_fundref).all()
    ror = institutions[institutions["organization_id_source"] == "ROR"]
    assert ror["organization_id"].apply(is_valid_ror).all()
    sha256 = institutions[institutions["organization_id_source"] == "sha256"]
    assert sha256["organization_id"].apply(is_valid_sha256).all()
