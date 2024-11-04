# flake8: noqa: D100, F403, F405
import argparse
import logging
import pathlib

import pandas as pd
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from tqdm import tqdm

from citations.neo4j import utils
from citations.neo4j.loader import *

logger = logging.getLogger(__name__)


def main(args: argparse.Namespace):
    """
    Integrate data into the Neo4j database.

    Parameters
    ----------
    args
        Command line arguments
    """
    data_path = args.data_path
    uri = args.uri
    user = args.user
    password = args.password
    batch_size = args.batch_size

    auth = (user, password)
    driver = GraphDatabase.driver(uri, auth=auth)

    authors = pd.read_csv(data_path / "authors.csv")
    authors = authors.map(lambda x: None if pd.isna(x) else x)
    extended_articles = pd.read_json(data_path / "extended_articles.jsonl", lines=True)
    institutions = pd.read_csv(data_path / "institutions.csv")
    institutions = institutions.map(lambda x: None if pd.isna(x) else x)
    article_cites_article = pd.read_csv(data_path / "article_cites_article.csv")
    article_cites_article = article_cites_article.map(lambda x: None if pd.isna(x) else x)
    author_affiliated_with_institution = pd.read_csv(data_path / "author_affiliated_with_institution.csv")
    author_affiliated_with_institution = author_affiliated_with_institution.map(lambda x: None if pd.isna(x) else x)
    author_wrote_article = pd.read_csv(data_path / "author_wrote_article.csv")
    author_wrote_article = author_wrote_article.map(lambda x: None if pd.isna(x) else x)

    with driver.session(database=args.database, auth=auth) as session:
        session.execute_write(utils.count_all_nodes)
        session.execute_write(utils.count_all_edges)

        if args.wipe_db:
            logger.info("Removing all nodes and edges...")
            session.execute_write(utils.remove_all_nodes_and_edges)
            session.execute_write(utils.count_all_nodes)
            session.execute_write(utils.count_all_edges)
            logger.info("Removing constraints and indexes")
            session.execute_write(utils.drop_all_constraints_and_indexes)
        else:
            logger.info("Integrating data without resetting the database.")

        session.execute_write(create_constraints)
        session.execute_write(create_indexes)

    def preprocess_article(article: dict) -> dict:
        """
        Preprocess article data to handle optional fields.

        Parameters
        ----------
        article : dict
            The article data to preprocess.

        Returns
        -------
        dict
            The preprocessed article data.
        """
        article = process_coordinates(article, "umap")
        article = process_coordinates(article, "tsne")
        article = process_coordinates(article, "pca")

        return article

    def batch_process(data, batch_function, batch_size, preprocess_function=None):
        """
        Process data in batches.

        Parameters
        ----------
        data : DataFrame
            The data to be processed in batches.
        batch_function : function
            The function to be applied to each batch.
        batch_size : int
            The size of each batch.
        preprocess_function : function, optional
            A function to preprocess each item in the batch.
        """
        for i in tqdm(range(0, len(data), batch_size), desc="Processing batches"):
            batch = data[i : i + batch_size].to_dict(orient="records")
            if preprocess_function:
                batch = [preprocess_function(item) for item in batch]
            with driver.session(database=args.database, auth=auth) as session:
                try:
                    session.execute_write(batch_function, batch)
                except Neo4jError as e:
                    logging.error(f"Neo4j Error in batch {i // batch_size}: {e.message}")
                except Exception as e:
                    logging.error(f"Unexpected error in batch {i // batch_size}: {str(e)}")

    try:
        logging.info("Processing extended articles...")
        batch_process(
            extended_articles,
            batch_add_articles,
            batch_size,
            preprocess_function=preprocess_article,
        )

        logging.info("Processing authors...")
        batch_process(authors, batch_add_authors, batch_size)

        # ... other batch_process calls ...

    except Exception as e:
        logging.error(f"An error occurred during data processing: {str(e)}")

    create_vector_index(driver, "article_embeddings", "Article", "embedding", 3072, "cosine")
    batch_process(institutions, batch_add_institutions, batch_size)

    article_connections = article_cites_article.rename(
        columns={
            "article_uid_source": "source",
            "article_uid_target": "target",
        }
    )
    batch_process(article_connections, batch_add_article_cites_article, batch_size)

    author_institution_connections = author_affiliated_with_institution.rename(
        columns={"author_uid": "author", "institution_uid": "institution"}
    )
    batch_process(
        author_institution_connections,
        batch_add_author_affiliated_with_institution,
        batch_size,
    )

    author_article_connections = author_wrote_article.rename(columns={"author_uid": "author", "article_uid": "article"})
    batch_process(
        author_article_connections,
        batch_add_author_wrote_article,
        batch_size,
    )
    create_vector_index(driver, "article_embeddings", "Article", "embedding", 1536, "cosine")

    with driver.session(database=args.database, auth=auth) as session:
        session.execute_write(add_author_cites_article)  # Edge
        session.execute_write(add_institution_cites_article)  # Edge
        session.execute_write(add_author_wrote_bbp)  # Boolean Property
        session.execute_write(set_current_affiliation)  # Boolean Property
        session.execute_write(add_author_num_articles_written)  # Numerical Property
        session.execute_write(add_num_bbp_articles_written)  # Numerical Property
        session.execute_write(add_institution_num_bbp_articles_cites)  # Numerical Property
        session.execute_write(add_article_num_bbp_articles_cites)  # Numerical Property
        session.execute_write(add_author_num_bbp_articles_cites)  # Numerical Property
        session.execute_write(add_num_ex_aff_authors)  # Numerical Property
        session.execute_write(add_num_ex_aff_bbp_authors)  # Numerical Property
        session.execute_write(add_num_currently_aff_authors)  # Numerical Property
        session.execute_write(add_num_currently_aff_bbp_authors)  # Numerical Property
        session.execute_write(add_num_citing_authors)  # Numerical Property
        session.execute_write(add_num_citing_institutions)  # Numerical Property

    logger.info("Data import complete.")
    with driver.session(database=args.database, auth=auth) as session:
        session.execute_write(utils.count_all_nodes)
        session.execute_write(utils.count_all_edges)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    """Run Script"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "data_path",
        type=pathlib.Path,
        help="Path to the directory containing the CSV files.",
    )
    parser.add_argument("uri", help="URI of the Neo4j instance.")
    parser.add_argument("user", help="Username of the Neo4j instance.")
    parser.add_argument("password", help="Password of the Neo4j instance.")
    parser.add_argument("--database", help="NEO4J database to connect to.", default="neo4j")
    parser.add_argument(
        "--wipe_db",
        help="Delete db nodes, edges, indexes, constraints.",
        action="store_true",
    )
    parser.add_argument(
        "--batch_size",
        help="Batch size to use during article integration.",
        type=int,
        default=1000,
    )

    args = parser.parse_args()

    main(args)
