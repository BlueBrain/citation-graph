"""Fetch embeddings from Neo4j and create attributes for reduced dimension positions."""

import argparse
import logging

import numpy as np
import umap
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)


# TODO: this should be saving a json file with the UMAP coordinates instead of embedding them in the neo4j database
def fetch_embeddings(uri, user, password):
    """Fetch embeddings from Neo4j.

    Args
    ----
        uri (str): Neo4j URI
        user (str): Neo4j user
        password (str): Neo4j password

    Returns
    -------
        embeddings (list): List of embeddings
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        result = session.run(
            """
        MATCH (a:Article)
        RETURN a.uid AS uid, a.embedding AS embedding
        """
        )
        embeddings = [(record["uid"], record["embedding"]) for record in result]
    driver.close()
    return embeddings


def generate_umap_coordinates(embeddings, n_components=2, random_state=42):
    """Generate UMAP coordinates for articles.

    Args
    ----
        embeddings (list): List of embeddings
        n_components (int): Number of UMAP components
        random_state (int): Random state for UMAP

    Returns
    -------
        uid_to_umap (dict): Dictionary of UMAP coordinates
    """
    uids, embedding_vectors = zip(*embeddings)
    embedding_matrix = np.array(embedding_vectors)
    umap_model = umap.UMAP(n_components=n_components, random_state=random_state)
    umap_coordinates = umap_model.fit_transform(embedding_matrix)
    return dict(zip(uids, umap_coordinates))


def store_umap_coordinates(uri, user, password, uid_to_umap):
    """Store UMAP coordinates into Articles.

    Args
    ----
        uri (str): Neo4j URI
        user (str): Neo4j user
        password (str): Neo4j password
        uid_to_umap (dict): Dictionary of UMAP coordinates

    Returns
    -------
        None
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        for uid, coord in uid_to_umap.items():
            session.run(
                """
            MATCH (a:Article {uid: $uid})
            SET a.umap_x = $x, a.umap_y = $y
            """,
                uid=uid,
                x=coord[0],
                y=coord[1],
            )
    driver.close()


def main(args):
    """Generate UMAP coordinates for articles and store them in Neo4j."""
    logging.info("Fetching embeddings from Neo4j...")
    embeddings = fetch_embeddings(args.uri, args.user, args.password)
    logging.info("Generating UMAP coordinates...")
    uid_to_umap = generate_umap_coordinates(embeddings, args.n_components, args.random_state)
    logging.info("Storing UMAP coordinates into Articles...")
    store_umap_coordinates(args.uri, args.user, args.password, uid_to_umap)


if __name__ == "__main__":
    """
    Main function to generate UMAP coordinates for articles and store them in Neo4j.
    Run:
    python dimension_reduction.py --uri bolt://localhost:7687 --user neo4j --password password
    """
    parser = argparse.ArgumentParser(description=("Generate UMAP coordinates for articles and store them in Neo4j."))
    parser.add_argument(
        "--uri",
        type=str,
        default="bolt://localhost:7687",
        required=True,
        help="Neo4j URI",
    )
    parser.add_argument("--user", type=str, default="neo4j", required=True, help="Neo4j user")
    parser.add_argument(
        "--password",
        type=str,
        required=True,
        default="password",
        help="Neo4j password",
    )
    parser.add_argument(
        "--n_components",
        type=int,
        default=2,
        help="Number of UMAP components (default: 2)",
    )
    parser.add_argument(
        "--random_state",
        type=int,
        default=42,
        help="Random state for UMAP (default: 42)",
    )

    args = parser.parse_args()
    main(args)
