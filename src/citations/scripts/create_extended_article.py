"""Creating extended article."""

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pydantic_core
import yaml
from tqdm import tqdm

from citations.schemas import ClusterAnalysis, DimensionReductionResult, ExtendedArticle

logging.basicConfig(level=logging.INFO)


def get_cluster_id(clusters: Dict[int, List[str]], uid: str) -> int:
    """
    Get the cluster ID for a given article UID.

    Parameters
    ----------
    clusters
        Dictionary of clusters where the key is the cluster ID and the value is the list of article UIDs.
    uid
        UID of the article.

    Returns
    -------
    int
        Cluster ID for the article.
    """
    for cluster_id, article_uids in clusters.items():
        if uid in article_uids:
            return cluster_id
    return -1  # Return -1 if the UID is not found in any cluster


def load_clusters(paths: list) -> Dict[str, ClusterAnalysis]:
    """
    Load cluster data from multiple paths.

    Parameters
    ----------
    paths
        List of paths to cluster files.

    Returns
    -------
    dict
        Dictionary of ClusterAnalysis objects keyed by a combination of algorithm and parameters.
    """
    clusters = {}
    for path in paths:
        with open(path, "r", encoding="utf-8") as file:
            cluster_data = json.load(file)
            cluster_analysis = ClusterAnalysis(**cluster_data)
            key = f"{cluster_analysis.algorithm}_{json.dumps(
                cluster_analysis.parameters, sort_keys=True)}"
            clusters[key] = cluster_analysis
    return clusters


def load_dimension_reduction(path: str) -> DimensionReductionResult:
    """
    Load dimension reduction data from a JSON file.

    Parameters
    ----------
    path
        Path to the JSON file.

    Returns
    -------
    DimensionReductionResult
        Dimension reduction data.
    """
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)
        return DimensionReductionResult(**data)


def main(args):
    """Run the script."""
    with open(args.config_path, "r") as file:
        config = yaml.safe_load(file)

    articles_path = config["articles_path"]
    embeddings_path = config["embeddings_path"]
    clusters_config = config["clusters"]
    dimension_reduction_config = config["dimension_reduction"]
    output_path = config["output_path"]

    articles_df = pd.read_csv(articles_path)
    cluster_data = {}
    for cluster_method, paths in clusters_config.items():
        cluster_data[cluster_method] = load_clusters(paths)

    # Load UMAP dimension reduction data
    umap_data = load_dimension_reduction(dimension_reduction_config["umap"])
    umap_dict = dict(zip(umap_data.article_uids, umap_data.reduced_dimensions))

    extended_articles = []
    extended_article_mappings = {}
    for _, row in tqdm(articles_df.iterrows(), desc="Creating articles"):
        article_dict = dict(row)
        article_dict["doi"] = None if pd.isna(article_dict["doi"]) else article_dict["doi"]
        article_dict["abstract"] = None if pd.isna(article_dict["abstract"]) else article_dict["abstract"]
        article_dict["pmid"] = None if pd.isna(article_dict["pmid"]) else str(int(article_dict["pmid"]))
        article_dict["isbns"] = None if pd.isna(article_dict["isbns"]) else article_dict["isbns"]
        article_dict["europmc_id"] = (
            str(article_dict["europmc_id"]) if not pd.isna(article_dict["europmc_id"]) else None
        )
        article_dict["citations"] = int(article_dict["citations"]) if not pd.isna(article_dict["citations"]) else 0
        article_dict["google_scholar_id"] = (
            None if pd.isna(article_dict["google_scholar_id"]) else article_dict["google_scholar_id"]
        )

        if article_dict["is_published"] is False and args.ignore_unpublished:
            logging.info(f"Skipping article {article_dict['uid']} as it is not" " published.")
            continue

        try:
            # convert nan to None
            article_dict = {key: None if pd.isna(value) else value for key, value in article_dict.items()}
            extended_article = ExtendedArticle(**article_dict)

        except pydantic_core._pydantic_core.ValidationError as e:
            logging.error(f"Pydantic Schema error on Extended Article: {e}")
            continue
        except Exception as e:
            logging.error(f"Unexpected Error creating extended article: {e}")
            continue

        for cluster_method, cluster_analyses in cluster_data.items():
            clusters = {
                key: get_cluster_id(cluster_analysis.clusters, row.uid)
                for key, cluster_analysis in cluster_analyses.items()
            }
            setattr(extended_article, f"{cluster_method}_clusters", clusters)

        # Add UMAP coordinates
        if row.uid in umap_dict:
            extended_article.umap = umap_dict[row.uid]
        else:
            logging.debug(f"UMAP coordinates not found for article {row.uid}")

        extended_article_mappings[row.uid] = extended_article
        extended_articles.append(extended_article)

    # New block for handling embeddings
    with open(embeddings_path, "r", encoding="utf-8") as embeddings_file:
        for line in tqdm(embeddings_file, desc="Adding embeddings"):
            embedding_json = json.loads(line)
            try:
                extended_article = extended_article_mappings[embedding_json["article_uid"]]
                extended_article.embedding = embedding_json["vector"]
            except KeyError:
                logging.warning("Embedding not found for article:" f" {embedding_json['article_uid']}")

    # Set embedding to None for articles without embeddings
    for article in extended_articles:
        if not hasattr(article, "embedding"):
            article.embedding = None

    # Write extended articles to JSON file
    with open(output_path, "w", encoding="utf-8") as articles_file:
        for article in tqdm(extended_articles, desc="Writing extended articles"):
            articles_file.write(article.json() + "\n")

    logging.info(f"Extended articles saved to {output_path}")
    return extended_articles


def get_parser() -> argparse.ArgumentParser:
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "config_path",
        type=Path,
        help="Path to the configuration",
    )
    parser.add_argument(
        "--ignore-unpublished",
        action="store_true",
        help="Ignore articles that are not published",
    )

    return parser


if __name__ == "__main__":
    """Run the script."""
    parser = get_parser()
    args = parser.parse_args()

    main(args)
