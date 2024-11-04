"""Tests for author wrote article."""


def test_unique_records(author_wrote_article):
    """Ensure that each record is unique."""
    assert not author_wrote_article.duplicated().any()


def test_authors_articles_exist(articles, authors, author_wrote_article):
    """Ensure that each author and article exists."""
    assert author_wrote_article["author_uid"].isin(authors["uid"]).all()
    assert author_wrote_article["article_uid"].isin(articles["uid"]).all()
