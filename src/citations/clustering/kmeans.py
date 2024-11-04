"""KMeans clustering module."""

import logging
import time
from typing import Dict, List

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import (
    calinski_harabasz_score,
    davies_bouldin_score,
    silhouette_score,
)

from citations.schemas import ClusterAnalysis

logging.basicConfig(level=logging.INFO)


def run_kmeans_clustering(
    embeddings: Dict[str, List[float]],
    **kwargs,
) -> ClusterAnalysis:
    """Run KMeans clustering on the embeddings.

    Parameters
    ----------
    embeddings : Dict[str, List[float]]
        A dictionary of embeddings.
    Optional Parameters
    -------------------
    n_clusters : int
        The number of clusters to form as well as the number of centroids to generate.
    random_state : int, optional
        Determines random number generation for centroid initialization.

    Returns
    -------
    ClusterAnalysis
        The cluster analysis object.
    """
    clustering_model = KMeans(**kwargs)

    logging.info("Running KMeans clustering with params" f" {clustering_model.get_params()}...")
    st_time = time.time()
    # Convert embeddings to numpy array
    embedding_array = np.array(list(embeddings.values()))
    cluster_labels = clustering_model.fit_predict(embedding_array)
    logging.info(f"Clustering took {time.time() - st_time:.2f} seconds.")

    logging.info("Calculating evaluation metrics...")
    if len(set(cluster_labels)) > 1:
        silhouette_score_ = silhouette_score(list(embeddings.values()), cluster_labels, metric="cosine")
        logging.info(f"Silhouette score: {silhouette_score_}")
        logging.info(f"Number of clusters: {len(set(cluster_labels))}")
        logging.info(f"Number of articles: {len(embeddings)}")

        # davies_bouldin_score
        davies_bouldin_score_ = davies_bouldin_score(list(embeddings.values()), cluster_labels)
        logging.info(f"Davies-Bouldin score: {davies_bouldin_score_}")

        # calinski_harabasz_score
        calinski_harabasz_score_ = calinski_harabasz_score(list(embeddings.values()), cluster_labels)
        logging.info(f"Calinski-Harabasz score: {calinski_harabasz_score_}")
    else:
        silhouette_score_ = np.nan
        davies_bouldin_score_ = np.nan
        calinski_harabasz_score_ = np.nan

    logging.info("Creating cluster analysis object...")
    params = {k: str(v) for k, v in kwargs.items()}
    cluster_analysis = ClusterAnalysis(
        algorithm="KMeans",
        parameters=params,
        clusters={i: [] for i in set(cluster_labels) if i != -1},
        silhouette_score=silhouette_score_,
        davies_bouldin_score=davies_bouldin_score_,
        calinski_harabasz_score=calinski_harabasz_score_,
    )

    logging.info("Mapping articles to clusters...")
    for uid, label in zip(embeddings.keys(), cluster_labels):
        if label != -1:
            cluster_analysis.clusters[label].append(uid)

    return cluster_analysis
