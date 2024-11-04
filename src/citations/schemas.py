"""Node and relation schemas."""

from datetime import date
from typing import Dict, List, Literal

from pydantic import BaseModel, ConfigDict

OrganizationIdSource = Literal[
    "LEI", "FUNDREF", "GRID", "RINGGOLD", "ROR", "sha256"
]


class Article(BaseModel):
    """Article schema."""

    model_config = ConfigDict(extra="forbid")
    uid: str
    title: str
    source: Literal["europmc", "csv", "serp", "serp_europmc", "serp_csv"]
    is_bbp: bool
    is_published: bool
    publication_date: date | None = None
    abstract: str | None = None
    doi: str | None = None
    pmid: str | None = None
    europmc_id: str | None = None
    google_scholar_id: str | None = None
    url: str | None = None
    isbns: str | None = None
    citations: int | None = None


class Author(BaseModel):
    """Author schema."""

    model_config = ConfigDict(extra="forbid")
    uid: str
    orcid_id: str | None
    name: str | None = None
    google_scholar_id: str | None = None


class Institution(BaseModel):
    """Institution schema."""

    model_config = ConfigDict(extra="forbid")
    uid: str
    name: str
    organization_id: str
    organization_id_source: OrganizationIdSource


class ArticleCitesArticle(BaseModel):
    """Article cites article schema."""

    model_config = ConfigDict(extra="forbid")
    article_uid_source: str
    article_uid_target: str


class AuthorWroteArticle(BaseModel):
    """Author wrote article schema."""

    model_config = ConfigDict(extra="forbid")
    author_uid: str
    article_uid: str


class AuthorAffiliatedWithInstitution(BaseModel):
    """Author affiliated with institution schema."""

    model_config = ConfigDict(extra="forbid")
    author_uid: str
    institution_uid: str
    start_date: date | None
    end_date: date | None


# ANALYSIS DATA


class ClusterAnalysis(BaseModel):
    """Cluster analysis schema."""

    model_config = ConfigDict(extra="forbid")
    algorithm: str
    parameters: Dict[str, str]
    clusters: Dict[int, List[str]]  # Cluster ID mapped to List of article UIDs
    silhouette_score: float | None = None
    davies_bouldin_score: float | None = None
    calinski_harabasz_score: float | None = None


class EmbeddingMetadata(BaseModel):
    """Metadata for embeddings."""

    article_uid: str
    model: str


class UMAPParams(BaseModel):
    """UMAP parameters schema."""

    n_neighbors: int = 15
    n_components: int = 2
    metric: str = "euclidean"
    min_dist: float = 0.1
    random_state: int = 42


class DimensionReductionResult(BaseModel):
    """Dimension reduction result schema."""

    method: str
    params: UMAPParams  # TODO: generalize but not urgent
    article_uids: List[str]
    reduced_dimensions: List[List[float]]


class ExtendedArticle(Article):
    """Article with analysis data schema."""

    model_config = ConfigDict(extra="forbid")

    # embeddings
    embedding: List[float] | None = None

    # clusters
    dbscan_clusters: Dict[int, int] | None = None
    hdbscan_clusters: Dict[int, int] | None = None
    kmeans_clusters: Dict[int, int] | None = None
    agglomerative_clusters: Dict[int, int] | None = None

    # topics
    lda_topic: str | None = None
    nmf_topic: str | None = None

    # dimensionality reduction
    tsne: List[float] | None = None
    pca: List[float] | None = None
    umap: List[float] | None = None
