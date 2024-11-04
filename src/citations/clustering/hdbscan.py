"""HDBSCAN clustering module."""

import logging
import time
from typing import Dict, List

import numpy as np
from sklearn.cluster import HDBSCAN
from sklearn.metrics import (
    calinski_harabasz_score,
    davies_bouldin_score,
    silhouette_score,
)

from citations.schemas import ClusterAnalysis

logging.basicConfig(level=logging.INFO)


def run_hdbscan_clustering(
    embeddings: Dict[str, List[float]],
    **kwargs,
) -> ClusterAnalysis:
    """Run HDBSCAN clustering on the embeddings.

    Parameters
    ----------
    embeddings : Dict[str, List[float]]
        A dictionary of embeddings.
    min_cluster_size : int
        The smallest size grouping that should be considered a cluster.
    min_samples : int
        The number of samples in a neighborhood for a point to be considered as a core point.

    Returns
    -------
    ClusterAnalysis
        The cluster analysis object.
    """
    clustering_model = HDBSCAN(**kwargs)

    logging.info("Running HDBSCAN clustering with params" f" {clustering_model.get_params()}...")
    st_time = time.time()
    # check if embeddings has duplicate keys
    embedding_array = np.array(list(embeddings.values()))
    cluster_labels = clustering_model.fit_predict(embedding_array)
    logging.info(f"Clustering took {time.time() - st_time:.2f} seconds.")

    logging.info("Calculating evaluation metrics...")
    labels = np.array([i for i in range(len(set(cluster_labels))) if len(set(cluster_labels)) > 1])
    logging.info(f"Number of clusters: {len(labels)}")

    if len(labels) > 1:
        silhouette_score_ = silhouette_score(list(embeddings.values()), cluster_labels, metric="cosine")
        logging.info(f"Silhouette score: {silhouette_score_}")
        logging.info(f"Number of clusters: {len(labels)}")
        logging.info(f"Number of noise points: {len(set(cluster_labels)) - len(labels)}")
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
        algorithm="HDBSCAN",
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
