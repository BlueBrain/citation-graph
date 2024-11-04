"""Configuration for data tests."""

import json
import os

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def articles():
    """Return articles data."""
    return pd.read_csv(os.path.join("data", "articles.csv"), dtype={"pmid": str})


@pytest.fixture(scope="session")
def authors():
    """Return authors data."""
    return pd.read_csv(os.path.join("data", "authors.csv"))


@pytest.fixture(scope="session")
def institutions():
    """Return institutions data."""
    return pd.read_csv(os.path.join("data", "institutions.csv"))


@pytest.fixture(scope="session")
def article_cites_article():
    """Return citation data."""
    return pd.read_csv(os.path.join("data", "article_cites_article.csv"))


@pytest.fixture(scope="session")
def author_wrote_article():
    """Return author wrote data."""
    return pd.read_csv(os.path.join("data", "author_wrote_article.csv"))


@pytest.fixture(scope="session")
def author_affiliated_with_institution():
    """Return affiliation data."""
    return pd.read_csv(os.path.join("data", "author_affiliated_with_institution.csv"))


@pytest.fixture(scope="session")
def embedded_article_uids():
    """Return embedding uids."""
    uids = []
    with open(os.path.join("data", "articles_embedded.jsonl"), "r", encoding="utf-8") as articles_file:
        for line in articles_file:
            embedded_article = json.loads(line)
            uids.append(embedded_article["article_uid"])
    return uids


@pytest.fixture(scope="session")
def clusterings():
    """Return all clusterings."""
    clustering_dir = os.path.join("data", "clustering")
    clusterings = {}
    for clustering in os.listdir(clustering_dir):
        clustering_path = os.path.join(clustering_dir, clustering)
        with open(clustering_path, "r", encoding="utf-8") as clustering_file:
            clusterings[clustering] = json.load(clustering_file)
    return clusterings
