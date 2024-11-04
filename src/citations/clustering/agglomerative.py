"""Agglomerative clustering module."""

import logging
import time
from typing import Dict, List

import numpy as np
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import (
    calinski_harabasz_score,
    davies_bouldin_score,
    silhouette_score,
)

from citations.schemas import ClusterAnalysis

logging.basicConfig(level=logging.INFO)


def run_agglomerative_clustering(
    embeddings: Dict[str, List[float]],
    **kwargs,
) -> ClusterAnalysis:
    """Run agglomerative clustering on the embeddings.

    Parameters
    ----------
    embeddings : Dict[str, List[float]]
        A dictionary of embeddings.
    Optional Parameters
    -------------------
    n_clusters : int
        The number of clusters to create.
    metric : str, optional
        The distance metric to use, by default "cosine".
    linkage : str, optional
        The linkage criterion to use, by default "ward".

    Returns
    -------
    ClusterAnalysis
        The cluster analysis object.
    """
    clustering_model = AgglomerativeClustering(**kwargs)

    logging.info(
        "Running Agglomerative clustering with params"
        f" {clustering_model.get_params()}..."
    )
    st_time = time.time()
    # Convert embeddings to numpy array
    embedding_array = np.array(list(embeddings.values()))
    # Fit the model and predict the cluster labels
    cluster_labels = clustering_model.fit_predict(embedding_array)
    logging.info(f"Clustering took {time.time() - st_time:.2f} seconds.")

    # Calculate evaluation metrics
    logging.info("Calculating evaluation metrics...")
    labels = np.array(
        [
            i
            for i in range(len(set(cluster_labels)))
            if len(set(cluster_labels)) > 1
        ]
    )
    logging.info(f"Number of clusters: {len(labels)}")

    if len(set(cluster_labels)) > 1:
        silhouette_score_ = silhouette_score(
            embedding_array, cluster_labels, metric="cosine"
        )
        davies_bouldin_score_ = davies_bouldin_score(
            embedding_array, cluster_labels
        )
        calinski_harabasz_score_ = calinski_harabasz_score(
            embedding_array, cluster_labels
        )
        logging.info(f"Silhouette score: {silhouette_score_}")
        logging.info(f"Davies-Bouldin score: {davies_bouldin_score_}")
        logging.info(f"Calinski-Harabasz score: {calinski_harabasz_score_}")
    else:
        silhouette_score_ = np.nan
        davies_bouldin_score_ = np.nan
        calinski_harabasz_score_ = np.nan

    # Create the ClusterAnalysis object
    logging.info("Creating cluster analysis object...")
    params = {k: str(v) for k, v in kwargs.items()}
    cluster_analysis = ClusterAnalysis(
        algorithm="AgglomerativeClustering",
        parameters=params,
        clusters={i: [] for i in set(cluster_labels)},
        silhouette_score=silhouette_score_,
        davies_bouldin_score=davies_bouldin_score_,
        calinski_harabasz_score=calinski_harabasz_score_,
    )

    # Assign articles to clusters
    logging.info("Mapping articles to clusters...")
    for uid, label in zip(embeddings.keys(), cluster_labels):
        cluster_analysis.clusters[label].append(uid)

    return cluster_analysis
