"""Loader functions for Neo4j database."""

import json
import logging

import pandas as pd
from neo4j import ManagedTransaction
from neo4j.exceptions import Neo4jError

CLUSTERING_ALGORITHMS = [
    "agglomerative_clusters",
    "kmeans_clusters",
]


def create_constraints(tx: ManagedTransaction) -> None:
    """
    Create constraints in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    tx.run("CREATE CONSTRAINT FOR (p:Article) REQUIRE p.uid IS UNIQUE")
    tx.run("CREATE CONSTRAINT FOR (a:Author) REQUIRE a.uid IS UNIQUE")
    tx.run("CREATE CONSTRAINT FOR (i:Institution) REQUIRE i.uid IS UNIQUE")
    tx.run("CREATE CONSTRAINT FOR (k:Keyword) REQUIRE k.name IS UNIQUE")


def create_indexes(tx: ManagedTransaction) -> None:
    """
    Create indexes in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    tx.run("CREATE INDEX FOR (p:Article) ON (p.title)")
    tx.run("CREATE INDEX FOR (a:Author) ON (a.name)")
    tx.run("CREATE INDEX FOR (i:Institution) ON (i.name)")
    tx.run("CREATE INDEX FOR (a:Article) ON (a.is_bbp)")
    tx.run("CREATE INDEX FOR (a:Article) ON (a.publication_date)")
    tx.run("CREATE INDEX FOR (c:Cluster) ON (c.algorithm)")
    tx.run("CREATE INDEX FOR (a:Author) ON (a.google_scholar_id)")
    tx.run("CREATE INDEX FOR (a:Article) ON (a.google_scholar_id)")


def create_vector_index(driver, index_name, label, property_name, dimensions, similarity_function):
    """
    Create a vector index in Neo4j.

    Args
    ----
        driver: The Neo4j driver instance.
        index_name: The name of the index.
        label: The label of the nodes to index.
        property_name: The name of the property containing the vector.
        dimensions: The dimensionality of the vectors.
        similarity_function: The similarity function to use (e.g., 'cosine').
    """
    with driver.session() as session:
        session.run(
            f"""
            CREATE VECTOR INDEX {index_name} IF NOT EXISTS
            FOR (m:{label})
            ON m.{property_name}
            OPTIONS {{indexConfig: {{
                `vector.dimensions`: {dimensions},
                `vector.similarity_function`: '{similarity_function}'
            }}}}
        """
        )


def process_coordinates(article, coord_name):
    """
    Process coordinates for a given coordinate name.

    Parameters
    ----------
    article : dict
        The article data to preprocess.
    coord_name : str
        The name of the coordinate set to process (e.g., "umap", "tsne", "pca").

    Returns
    -------
    dict
        The article data with processed coordinates.
    """
    if (
        coord_name in article
        and article[coord_name] is not None
        and isinstance(article[coord_name], list)
        and len(article[coord_name]) == 2
    ):
        article[f"{coord_name}_x"] = article[coord_name][0]
        article[f"{coord_name}_y"] = article[coord_name][1]
    else:
        article[f"{coord_name}_x"] = None
        article[f"{coord_name}_y"] = None

    return article


def add_coordinates_to_query(query: str, articles: list[dict], coord_name: str) -> str:
    """
    Add coordinates to the query if they are present in the articles.

    Parameters
    ----------
    query
        The query to add the coordinates to.
    articles
        A list of article dictionaries.
    coord_name
        The name of the coordinate system. e.g. umap, tsne, pca
    """
    x_key = f"{coord_name}_x"
    y_key = f"{coord_name}_y"
    if any(x_key in article and y_key in article for article in articles) and all(
        not pd.isna(article.get(x_key)) and not pd.isna(article.get(y_key)) for article in articles
    ):
        logging.info(f"Found {coord_name.upper()} coordinates")
        query += f""",
        a.{x_key} = article.{x_key},
        a.{y_key} = article.{y_key}
        """
    return query


def execute_query_with_logging(tx, query, params=None):
    """
    Execute a Neo4j query with logging.

    Args:
    ----
        tx (neo4j.Transaction): The transaction object used to run the query.
        query (str): The Cypher query to be executed.
        params (dict, optional): Parameters for the query. Defaults to None.

    Returns:
    -------
        neo4j.Result: The result of the query execution.

    Raises:
    ------
        Neo4jError: If there is an error executing the query in Neo4j.
        Exception: For any other unexpected errors.
    """
    try:
        result = tx.run(query, params)
        summary = result.consume()
        logging.debug("Query executed successfully. Affected records:" f" {summary.counters}")
        return result
    except Neo4jError as e:
        logging.error(f"Neo4j Error: {e.message}. Query: {query}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}. Query: {query}")
        raise


def batch_add_articles(tx: ManagedTransaction, articles: list[dict]) -> None:
    """
    Batch add articles to the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    articles
        A list of article dictionaries
    """
    query = """
    UNWIND $articles as article
    MERGE (a:Article {uid: article.uid})
    SET a.title = article.title,
        a.publication_date = CASE WHEN article.publication_date IS NULL THEN NULL ELSE date(article.publication_date) END,
        a.source = article.source,
        a.is_bbp = article.is_bbp,
        a.abstract = article.abstract,
        a.doi = article.doi,
        a.pmid = article.pmid,
        a.europmc_id = article.europmc_id,
        a.url = article.url,
        a.isbns = article.isbns,
        a.embedding = article.embedding,
        a.citations = article.citations,
        a.google_scholar_id = article.google_scholar_id

    // If it's a BBP thesis, add the BBP_Thesis label
    WITH a, article
    WHERE article.is_bbp = true AND article.url CONTAINS 'epfl'
    SET a:BBP_Thesis
    REMOVE a:Article

    // If it's an unpublished article, add the Unpublished_Article label
    WITH a, article
    WHERE a:Article AND article.source = 'csv' NOT a.publication_date IS NOT NULL
    SET a:Unpublished_Article
    REMOVE a:Article

    // If it's a book or book chapter, add the Book label
    WITH a, article
    WHERE a:Article AND article.url IS NOT NULL AND (article.url CONTAINS 'book' OR article.url CONTAINS 'chapter')
    SET a:Book
    REMOVE a:Article
    """

    query = add_coordinates_to_query(query, articles, "umap")
    query = add_coordinates_to_query(query, articles, "tsne")
    query = add_coordinates_to_query(query, articles, "pca")

    execute_query_with_logging(tx, query, {"articles": articles})

    for article in articles:
        for algorithm in CLUSTERING_ALGORITHMS:
            if algorithm in article and article[algorithm]:
                for key, cluster_id in article[algorithm].items():
                    params = extract_parameters(key)
                    set_param_query = build_cluster_set_query(params)
                    match_param_query = build_cluster_match_query(params)

                    query_cluster = f"""
                    MERGE (c:Cluster {{algorithm: '{algorithm.replace('_clusters', '').upper()}', cluster_id: {cluster_id}, {match_param_query}}})
                    SET c.algorithm = '{algorithm.replace('_clusters', '').upper()}', c.cluster_id = {cluster_id}, {set_param_query}
                    """
                    execute_query_with_logging(tx, query_cluster)

                    query_relationship = f"""
                    MATCH (a:Article {{uid: '{article["uid"]}'}}), (c:Cluster {{algorithm: '{algorithm.replace('_clusters', '').upper()}', cluster_id: {cluster_id}, {match_param_query}}})
                    MERGE (a)-[:IN_CLUSTER]->(c)
                    """
                    execute_query_with_logging(tx, query_relationship)


def extract_parameters(key: str) -> dict:
    """
    Extract parameters from the key string from the clustering data.

    Parameters
    ----------
    key : str
        The key string containing algorithm parameters.

    Returns
    -------
    dict
        A dictionary with the extracted parameters.
    """
    try:
        params = json.loads(key.split("_", 1)[1])
    except (IndexError, json.JSONDecodeError) as e:
        logging.error(f"Error parsing key '{key}': {e}")
        params = {}
    return params


def build_cluster_set_query(params: dict) -> str:
    """
    Build the cluster SET query part based on parameters.

    Parameters
    ----------
    params : dict
        The parameters to include in the query.

    Returns
    -------
    str
        The cluster SET query part.
    """
    set_query = ", ".join([f'c.{param} = "{value}"' for param, value in params.items()])
    return set_query


def build_cluster_match_query(params: dict) -> str:
    """
    Build the cluster MATCH query part based on parameters.

    Parameters
    ----------
    params : dict
        The parameters to include in the query.

    Returns
    -------
    str
        The cluster MATCH query part.
    """
    match_query = ", ".join([f'{param}: "{value}"' for param, value in params.items()])
    return match_query


def batch_add_authors(tx: ManagedTransaction, authors: list[dict]) -> None:
    """
    Batch add authors to the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    authors
        A list of author dictionaries
    """
    query = """
    UNWIND $authors as author
    MERGE (a:Author {uid: author.uid})
    SET a.orcid_id = author.orcid_id,
        a.name = author.name,
        a.google_scholar_id = author.google_scholar_id
    """
    execute_query_with_logging(tx, query, {"authors": authors})


def batch_add_institutions(tx: ManagedTransaction, institutions: list[dict]) -> None:
    """
    Batch add institutions to the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    institutions
        A list of institution dictionaries
    """
    query = """
    UNWIND $institutions as institution
    MERGE (i:Institution {uid: institution.uid})
    SET i.name = institution.name,
        i.organization_id = institution.organization_id,
        i.organization_id_source = institution.organization_id_source
    """
    execute_query_with_logging(tx, query, {"institutions": institutions})


def batch_add_article_cites_article(tx: ManagedTransaction, connections: list[dict]) -> None:
    """
    Batch connect articles in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    connections
        A list of article connections
    """
    query = """
    UNWIND $connections as connection
    MATCH (source:Article {uid: connection.source}), (target:Article {uid: connection.target})
    MERGE (source)-[:ARTICLE_CITES_ARTICLE]->(target)
    """
    execute_query_with_logging(tx, query, {"connections": connections})


def add_num_citing_authors(tx: ManagedTransaction) -> None:
    """
    Add number of authors who cited this article.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (source:Author)-[:AUTHOR_CITES_ARTICLE]->(target:Article)
    WITH target, count(source) AS num_citing_authors
    SET target.num_citing_authors = num_citing_authors
    """
    execute_query_with_logging(tx, query)


def add_num_articles_cite(tx: ManagedTransaction) -> None:
    """
    Add a numerical property 'num_articles_cites' that tells how many article cites this article.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (source:Article)-[:ARTICLE_CITES_ARTICLE]->(target:Article)
    WITH target, count(source) AS num_articles_cite
    SET target.num_articles_cite = num_articles_cite
    """
    execute_query_with_logging(tx, query)


def add_institution_num_bbp_articles_cites(tx: ManagedTransaction) -> None:
    """
    Add a property 'num_bbp_articles_cites' that tells how many BBP articles an Institution cites.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (source:Institution)-[:INSTITUTION_CITES_ARTICLE]->(target:Article)
    WHERE target.is_bbp = true
    WITH source, count(target) AS num_bbp_articles_cites
    SET source.num_bbp_articles_cites = num_bbp_articles_cites
    """
    execute_query_with_logging(tx, query)


def add_article_num_bbp_articles_cites(tx: ManagedTransaction) -> None:
    """
    Add a property 'num_bbp_articles_cites' that tells how many BBP articles an Article cites.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (source:Article)-[:ARTICLE_CITES_ARTICLE]->(target:Article)
    WHERE target.is_bbp = true
    WITH source, count(target) AS num_bbp_articles_cites
    SET source.num_bbp_articles_cites = num_bbp_articles_cites
    """
    execute_query_with_logging(tx, query)


def add_author_num_bbp_articles_cites(tx: ManagedTransaction) -> None:
    """
    Add a property 'num_bbp_articles_cites' that tells how many BBP articles an Article cites.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (source:Author)-[:AUTHOR_CITES_ARTICLE]->(target:Article)
    WHERE target.is_bbp = true
    WITH source, count(target) AS num_bbp_articles_cites
    SET source.num_bbp_articles_cites = num_bbp_articles_cites
    """
    execute_query_with_logging(tx, query)


def batch_add_author_affiliated_with_institution(tx: ManagedTransaction, connections: list[dict]) -> None:
    """
    Batch connect authors to institutions in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    connections
        A list of author-institution connections
    """
    query = """
    UNWIND $connections as connection
    MATCH (author:Author {uid: connection.author}), (institution:Institution {uid: connection.institution})
    MERGE (author)-[rel:AFFILIATED_WITH]->(institution)
    SET rel.start_date = connection.start_date,
        rel.end_date = connection.end_date
    """
    execute_query_with_logging(tx, query, {"connections": connections})


def batch_add_author_wrote_article(tx: ManagedTransaction, connections: list[dict]) -> None:
    """
    Batch connect authors to articles in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    connections
        A list of author-article connections
    """
    query = """
    UNWIND $connections as connection
    MATCH (author:Author)
    WHERE author.uid = connection.author OR author.google_scholar_id = connection.author
    MATCH (article:Article)
    WHERE article.uid = connection.article OR article.google_scholar_id = connection.article
    MERGE (author)-[:WROTE]->(article)
    """
    execute_query_with_logging(tx, query, {"connections": connections})


def add_author_cites_article(tx: ManagedTransaction) -> None:
    """
    Add 'author cites article' edge.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (author:Author)-[:WROTE]->(article1:Article)-[:ARTICLE_CITES_ARTICLE]->(article2:Article)
    MERGE (author)-[:AUTHOR_CITES_ARTICLE]->(article2)
    """
    execute_query_with_logging(tx, query)


def add_author_wrote_bbp(tx: ManagedTransaction) -> None:
    """
    Add property for Author telling if they wrote any BBP article.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (a:Author)-[:WROTE]->(article:Article)
    SET a.wrote_bbp = false
    """
    execute_query_with_logging(tx, query)

    query = """
    MATCH (a:Author)-[:WROTE]->(article:Article)
    WHERE article.is_bbp = true
    SET a.wrote_bbp = true
    """
    execute_query_with_logging(tx, query)


def add_author_num_articles_written(tx: ManagedTransaction) -> None:
    """
    Add property for Author telling how many articles they wrote in total.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (author:Author)-[:WROTE]->(article:Article)
    WITH author, count(article) AS num_articles_written
    SET author.num_articles_written = num_articles_written
    """
    execute_query_with_logging(tx, query)


def add_num_bbp_articles_written(tx: ManagedTransaction) -> None:
    """
    Add property for Author telling how many BBP articles they wrote in total.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (author:Author)-[:WROTE]->(article:Article)
    WHERE article.is_bbp = true
    WITH author, count(article) AS num_bbp_articles_written
    SET author.num_bbp_articles_written = num_bbp_articles_written
    """
    execute_query_with_logging(tx, query)


def add_num_ex_aff_authors(tx: ManagedTransaction) -> None:
    """
    Add property for Institution that tells us how many Authors they were associated with.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (author:Author)-[:AFFILIATED_WITH]->(institution:Institution)
    WITH institution, count(author) AS num_ex_aff_authors
    SET institution.num_ex_aff_authors = num_ex_aff_authors
    """
    execute_query_with_logging(tx, query)


def add_num_currently_aff_authors(tx: ManagedTransaction) -> None:
    """
    Add property for Institution that tells us how many Authors are currently associated with them.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (author:Author)-[aff:AFFILIATED_WITH]->(institution:Institution)
    WHERE aff.current = true
    WITH institution, count(author) AS num_currently_aff_authors
    SET institution.num_currently_aff_authors = num_currently_aff_authors
    """
    execute_query_with_logging(tx, query)


def add_num_ex_aff_bbp_authors(tx: ManagedTransaction) -> None:
    """
    Add property for Institution that tells us how many ex-BBP Authors they were associated with.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (author:Author)-[:AFFILIATED_WITH]->(institution:Institution)
    WHERE author.wrote_bbp = true
    WITH institution, count(author) AS num_ex_aff_bbp_authors
    SET institution.num_ex_aff_bbp_authors = num_ex_aff_bbp_authors
    """
    execute_query_with_logging(tx, query)


def add_num_currently_aff_bbp_authors(tx: ManagedTransaction) -> None:
    """
    Add property for Institution that tells us how many BBP Authors are currently associated with them.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (author:Author)-[aff:AFFILIATED_WITH]->(institution:Institution)
    WHERE author.wrote_bbp = true AND aff.current = true
    WITH institution, count(author) AS num_currently_aff_bbp_authors
    SET institution.num_currently_aff_bbp_authors = num_currently_aff_bbp_authors
    """
    execute_query_with_logging(tx, query)


def add_institution_cites_article(tx: ManagedTransaction) -> None:
    """
    Add edges where article is related to the institution through an author.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (author:Author)-[aff:AFFILIATED_WITH]->(institution:Institution),
          (author)-[:AUTHOR_CITES_ARTICLE]->(article:Article)
    WHERE aff.start_date IS NOT NULL AND article.publication_date IS NOT NULL
    WITH author, institution, article, aff,
         duration.inDays(date(aff.start_date), date(article.publication_date)).days AS time_diff_days
    ORDER BY abs(time_diff_days) ASC
    WITH author, institution, article, COLLECT(aff)[0] AS closest_affiliation
    WHERE date(closest_affiliation.start_date) <= date(article.publication_date)
    WITH institution, article,
         closest_affiliation.start_date AS start_date,
         closest_affiliation.end_date AS end_date
    MERGE (institution)-[rel:INSTITUTION_CITES_ARTICLE]->(article)
    SET rel.start_date = CASE WHEN start_date IS NOT NULL THEN start_date ELSE null END,
        rel.end_date = CASE WHEN end_date IS NOT NULL THEN end_date ELSE null END
    """
    execute_query_with_logging(tx, query)


def add_num_citing_institutions(tx: ManagedTransaction) -> None:
    """
    Add property that tells us how many institutions there are where an affiliated author cited this article.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (institution:Institution)-[cites:INSTITUTION_CITES_ARTICLE]->(article:Article)
    WITH article, count(institution) AS num_citing_institutions
    SET article.num_citing_institutions = num_citing_institutions
    """
    execute_query_with_logging(tx, query)


def set_current_affiliation(tx: ManagedTransaction) -> None:
    """
    Flag the most recent affiliations of each author.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    query = """
    MATCH (author:Author)-[aff:AFFILIATED_WITH]->(institution:Institution)
    WITH author, institution, aff
    SET aff.current = false
    """
    execute_query_with_logging(tx, query)
    query = """
    MATCH (author:Author)-[aff:AFFILIATED_WITH]->(institution:Institution)
    WHERE aff.start_date IS NOT NULL
    WITH author, institution, aff, aff.start_date AS start_date
    ORDER BY author.uid, start_date DESC
    WITH author, COLLECT(aff)[0] AS latest_aff

    SET latest_aff.current = true
    """
    execute_query_with_logging(tx, query)
