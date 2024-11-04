"""Tests for citation."""


def test_unique_records(article_cites_article):
    """Ensure that each record is unique."""
    assert not article_cites_article.duplicated().any()


def test_articles_exist(articles, article_cites_article):
    """Ensure that each cited article exists."""
    assert (
        article_cites_article["article_uid_source"].isin(articles["uid"]).all()
    )
    assert (
        article_cites_article["article_uid_target"].isin(articles["uid"]).all()
    )


def test_no_self_citing(article_cites_article):
    """Ensure that no article cites itself."""
    assert not (
        article_cites_article["article_uid_source"]
        == article_cites_article["article_uid_target"]
    ).any()
