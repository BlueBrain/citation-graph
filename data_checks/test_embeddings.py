"""Tests for article embeddings."""


def test_articles_have_embeddings(articles, embedded_article_uids):
    """Ensure that each article has an embedding."""
    for _, row in articles.iterrows():
        assert row.uid in embedded_article_uids


def test_embeddings_have_articles(articles, embedded_article_uids):
    """Ensure that each embedding has an article."""
    article_uids = articles["uid"].values
    for uid in embedded_article_uids:
        assert uid in article_uids
