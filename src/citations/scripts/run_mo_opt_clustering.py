"""Run optimization on clustering hyperparams."""

import argparse
import json
import logging
from typing import Callable, Dict, List

import optuna  # Import Optuna
from optuna.samplers import NSGAIISampler

# Import clustering methods
from citations.clustering import (
    run_agglomerative_clustering,
    run_dbscan_clustering,
    run_hdbscan_clustering,
    run_kmeans_clustering,
)
from citations.embed import load_embeddings
from citations.schemas import ClusterAnalysis, ExtendedArticle

logging.basicConfig(level=logging.INFO)

# Mapping clustering algorithms to their respective functions
CLUSTERING_ALGORITHMS: Dict[str, Callable] = {
    "agglomerative": run_agglomerative_clustering,
    "kmeans": run_kmeans_clustering,
    "dbscan": run_dbscan_clustering,
    "hdbscan": run_hdbscan_clustering,
}


def optimize_clustering(algorithm: str, embeddings: Dict[str, List[float]], **kwargs):
    """Optimize the clustering algorithm using multi-objective Optuna."""

    def objective(trial):
        # Define hyperparameters to be optimized based on the algorithm
        if algorithm == "hdbscan":
            params = {
                "min_cluster_size": trial.suggest_int("min_cluster_size", 5, 50),
                "min_samples": trial.suggest_int("min_samples", 5, 20),
                "cluster_selection_epsilon": trial.suggest_float("cluster_selection_epsilon", 0.01, 0.1),
                "metric": trial.suggest_categorical("metric", ["euclidean", "chebyshev", "cosine"]),
            }
        elif algorithm == "dbscan":
            params = {
                "eps": trial.suggest_float("eps", 0.1, 1.0),
                "min_samples": trial.suggest_int("min_samples", 5, 50),
                "metric": trial.suggest_categorical("metric", ["euclidean", "chebyshev", "cosine"]),
                "p": trial.suggest_float("p", 1.5, 2.5),
            }
        elif algorithm == "kmeans":
            params = {
                "n_clusters": trial.suggest_int("n_clusters", 5, 50),
            }
        elif algorithm == "agglomerative":
            params = {
                "n_clusters": trial.suggest_int("n_clusters", 15, 100),
                "linkage": trial.suggest_categorical("linkage", ["ward", "complete", "average", "single"]),
            }

        # Run clustering with the suggested parameters
        cluster_analysis = run_clustering(embeddings, algorithm, **params)

        # Return multiple objectives for optimization
        return (
            cluster_analysis.silhouette_score,
            -cluster_analysis.davies_bouldin_score,  # Negate since lower is better
            cluster_analysis.calinski_harabasz_score,
        )

    # Create a multi-objective study
    study = optuna.create_study(
        storage=f"sqlite:////Users/kurban/Documents/bbp/citation-graph/data/clustering/dev/db_{
            algorithm}.sqlite3",
        study_name=algorithm,
        directions=[
            "maximize",
            "minimize",
            "maximize",
        ],  # Directions for each objective
        sampler=NSGAIISampler(),  # Use NSGAIISampler for multi-objective optimization
        load_if_exists=True,
    )
    study.optimize(objective, n_trials=50)

    logging.info(f"Best trial: {study.best_trials}")
    for trial in study.best_trials:
        logging.info(f"Best parameters: {trial.params}")
        logging.info(f"Best scores: {trial.values}")

    # Run final clustering with the best parameters
    best_params = study.best_trials[0].params
    cluster_analysis = run_clustering(embeddings, algorithm, **best_params)

    return cluster_analysis


def run_clustering(embeddings: Dict[str, List[float]], algorithm: str, **kwargs) -> ClusterAnalysis:
    """Run the specified clustering algorithm on the embeddings.

    Parameters
    ----------
    embeddings : Dict[str, List[float]]
        A dictionary of embeddings.
    algorithm : str
        The clustering algorithm to use.

    Returns
    -------
    ClusterAnalysis
        The cluster analysis object.
    """
    if algorithm not in CLUSTERING_ALGORITHMS:
        raise ValueError(f"Unsupported clustering algorithm: {algorithm}")

    clustering_func = CLUSTERING_ALGORITHMS[algorithm]
    return clustering_func(embeddings, **kwargs)


def save_results(file_path: str, extended_articles: List[ExtendedArticle]):
    """Save the extended articles to a JSON file.

    Parameters
    ----------
    file_path : str
        The path to the output JSON file.
    extended_articles : List[ExtendedArticle]
        The list of extended articles to save.

    Returns
    -------
    None
    """
    with open(file_path, "w") as f:
        json.dump([article.dict() for article in extended_articles], f, indent=2)


def main(
    input_file: str,
    output_file: str,
    algorithm: str,
    optimize: bool = False,
    **kwargs,
):
    """Run clustering on the embeddings and save the results to a file.

    Parameters
    ----------
    input_file : str
        Path to the input JSON file with embeddings.
    output_file : str
        Path to the output JSON file for extended articles.
    algorithm : str
        Clustering algorithm to use.
    optimize : bool
        Whether to perform hyperparameter optimization.
    kwargs : dict
        Additional keyword arguments for the clustering algorithm.

    Returns
    -------
    None
    """
    embeddings = load_embeddings(input_file)
    # Convert string values to appropriate data types
    for key, value in kwargs.items():
        try:
            if "." in value:
                kwargs[key] = float(value)
            elif value.lower() in ("true", "false"):
                kwargs[key] = value.lower() == "true"
            elif value.isnumeric():  # Check if value is numeric
                kwargs[key] = int(value)
        except ValueError:
            pass  # Leave as string if conversion fails

    if optimize:
        cluster_analysis = optimize_clustering(algorithm, embeddings, **kwargs)
    else:
        cluster_analysis = run_clustering(embeddings, algorithm, **kwargs)

    # use pydantic model_dump_json on cluster_analysis
    with open(output_file, "w") as f:
        json_content = cluster_analysis.model_dump_json(indent=2)
        f.write(json_content)

    logging.info(f"Results saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and cluster article embeddings.")
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to the input JSON file with embeddings.",
    )
    parser.add_argument(
        "output_file",
        type=str,
        help="Path to the output JSON file for extended articles.",
    )
    parser.add_argument(
        "algorithm",
        type=str,
        choices=["agglomerative", "kmeans", "dbscan", "hdbscan"],
        help="Clustering algorithm to use.",
    )
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="Optimize hyperparameters using Optuna.",
    )
    # Add the `kwargs` argument to the parser
    parser.add_argument(
        "kwargs",
        nargs="*",
        help="Additional keyword arguments for the clustering algorithm.",
    )
    args = parser.parse_args()
    kwargs_dict = dict(arg.split("=") for arg in args.kwargs)
    main(
        args.input_file,
        args.output_file,
        args.algorithm,
        args.optimize,
        **kwargs_dict,
    )
