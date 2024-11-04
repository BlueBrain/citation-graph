"""Tests for affiliation."""


def test_authors_institutions_exist(
    authors, institutions, author_affiliated_with_institution
):
    """Ensure that each author and institution exists."""
    assert (
        author_affiliated_with_institution["author_uid"]
        .isin(authors["uid"])
        .all()
    )
    assert (
        author_affiliated_with_institution["institution_uid"]
        .isin(institutions["uid"])
        .all()
    )
