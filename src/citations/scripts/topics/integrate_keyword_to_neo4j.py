"""Script to integrate keyword embeddings and UMAP coordinates into Neo4j."""

import argparse
import json
import logging
from typing import Dict, List

from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase

from citations.neo4j.loader import execute_query_with_logging

logging.basicConfig(level=logging.INFO)
load_dotenv()


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Load keyword embeddings and UMAP coordinates to Neo4j"
    )
    parser.add_argument(
        "--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI"
    )
    parser.add_argument("--neo4j-user", default="neo4j", help="Neo4j username")
    parser.add_argument(
        "--neo4j-password", default="password", help="Neo4j password"
    )
    parser.add_argument(
        "--embeddings-file",
        type=str,
        required=True,
        help="Path to JSONL file containing keyword embeddings",
    )
    parser.add_argument(
        "--umap-file",
        type=str,
        required=True,
        help="Path to JSON file containing UMAP coordinates",
    )
    parser.add_argument(
        "--article-keywords",
        type=str,
        required=True,
        help="Path to article keywords JSON file",
    )
    return parser.parse_args()


def load_embeddings(file_path: str) -> Dict[str, List[float]]:
    """Load keyword embeddings from a JSONL file."""
    embeddings = {}
    with open(file_path, "r") as f:
        for line in f:
            data = json.loads(line)
            embeddings[data["keyword"]] = data["vector"]
    return embeddings


def load_umap_coordinates(file_path: str) -> Dict[str, List[float]]:
    """Load UMAP coordinates from a JSON file."""
    with open(file_path, "r") as f:
        data = json.load(f)
        return dict(zip(data["keywords"], data["reduced_dimensions"]))


def load_article_keywords(file_path: str) -> Dict[str, List[str]]:
    """Load article keywords from a JSON file."""
    with open(file_path, "r") as f:
        return json.load(f)


def update_neo4j(
    driver: Driver,
    embeddings: Dict[str, List[float]],
    umap_coordinates: Dict[str, List[float]],
    article_keywords: Dict[str, List[str]],
):
    """Update Neo4j database with keywords, embeddings, and UMAP coordinates."""
    with driver.session() as session:
        # First, create or update Keyword nodes
        for article_id, keywords in article_keywords.items():
            for keyword in keywords:
                query = """
                MERGE (k:Keyword {name: $keyword})
                WITH k
                MATCH (a:Article {uid: $article_id})
                MERGE (a)-[:HAS_KEYWORD]->(k)
                """
                execute_query_with_logging(
                    session,
                    query,
                    {"keyword": keyword, "article_id": article_id},
                )

        logging.info("Keyword nodes created and linked to articles.")

        # Then, update embeddings and UMAP coordinates
        for keyword, embedding in embeddings.items():
            query = """
            MATCH (k:Keyword {name: $keyword})
            SET k.embedding = $embedding
            """
            execute_query_with_logging(
                session, query, {"keyword": keyword, "embedding": embedding}
            )

        for keyword, coordinates in umap_coordinates.items():
            x, y = coordinates[0], coordinates[1]
            query = """
            MATCH (k:Keyword {name: $keyword})
            SET k.umap_x = $x, k.umap_y = $y
            """
            execute_query_with_logging(
                session, query, {"keyword": keyword, "x": x, "y": y}
            )

        logging.info("Keyword embeddings and UMAP coordinates updated.")


def main():
    """Load keywords, embeddings, and UMAP coordinates to Neo4j."""
    args = parse_arguments()

    driver = GraphDatabase.driver(
        args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password)
    )

    embeddings = load_embeddings(args.embeddings_file)
    umap_coordinates = load_umap_coordinates(args.umap_file)
    article_keywords = load_article_keywords(args.article_keywords)

    try:
        update_neo4j(driver, embeddings, umap_coordinates, article_keywords)
        logging.info(
            "Keywords, embeddings, and UMAP coordinates loaded to Neo4j"
            " successfully."
        )
    except Exception as e:
        logging.error(f"An error occurred while updating Neo4j: {str(e)}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
