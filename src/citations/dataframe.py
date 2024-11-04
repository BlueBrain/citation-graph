"""Collection of constants for dataframes."""

ARTICLE_COLUMN_DTYPES = {
    "uid": str,
    "title": str,
    "publication_date": str,
    "source": str,
    "is_bbp": bool,
    "abstract": str,
    "doi": str,
    "pmid": str,
    "europmc_id": str,
    "url": str,
    "citations": int,
    "is_published": bool,
    "isbns": str,
}
ARTICLE_COLUMNS = ARTICLE_COLUMN_DTYPES.keys()

AUTHOR_COLUMNS = ["uid", "orcid_id", "google_scholar_id", "name"]

INSTITUTION_COLUMNS = [
    "uid",
    "name",
    "organization_id",
    "organization_id_source",
]

ARTICLE_CITES_ARTICLE_COLUMNS = ["article_uid_source", "article_uid_target"]

AUTHOR_WROTE_ARTICLE_COLUMNS = ["author_uid", "article_uid"]

AUTHOR_AFFILIATED_WITH_INSTITUTION_COLUMNS = ["author_uid", "institution_uid"]
