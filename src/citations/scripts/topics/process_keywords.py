"""Process keywords and extract topics from clusters."""

import argparse
import json
import logging
import os
from typing import Dict, List

from dotenv import load_dotenv
from langchain.chains import LLMChain
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from neo4j import GraphDatabase
from pydantic import SecretStr

logging.basicConfig(level=logging.INFO)
load_dotenv()


def parse_arguments():
    """
    Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Process keywords and extract topics")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI")
    parser.add_argument("--neo4j-user", default="neo4j", help="Neo4j username")
    parser.add_argument("--neo4j-password", default="password", help="Neo4j password")
    parser.add_argument(
        "--article-keywords",
        type=str,
        required=True,
        help="Path to article keywords JSON file",
    )
    parser.add_argument(
        "--merge-suggestions",
        type=str,
        help="Path to keyword merge suggestions JSONL file (optional)",
    )
    parser.add_argument(
        "--clusters",
        type=str,
        required=True,
        help="Path to clusters JSON file",
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        help="Skip applying merge suggestions even if provided",
    )
    # force run
    parser.add_argument(
        "--force-run",
        action="store_true",
        help="Force run the script even if the output files exist",
    )
    return parser.parse_args()


def load_merge_suggestions(file_path: str) -> Dict[str, List[str]]:
    """
    Load keyword merge suggestions from a JSONL file.

    Parameters
    ----------
    file_path : str
        Path to the JSONL file.

    Returns
    -------
    dict
        Dictionary of keyword merge suggestions where the key is the merged keyword and the value is the list of original keywords.
    """
    merge_suggestions = {}
    if file_path and os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        with open(file_path, "r") as f:
            for line in f:
                suggestion = json.loads(line)
                merge_suggestions.update(suggestion)
    return merge_suggestions


args = parse_arguments()

NEO4J_URI = args.neo4j_uri
NEO4J_USER = args.neo4j_user
NEO4J_PASSWORD = args.neo4j_password

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if OPENAI_API_KEY is None:
    raise ValueError("OPENAI_API_KEY must be set in the environment.")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def apply_merge_suggestions(
    article_keywords: Dict[str, List[str]],
    merge_suggestions: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """
    Apply keyword merge suggestions to the article keywords.

    Use a dictionary comprehension and set intersection for better performance and scalability.

    Parameters
    ----------
    article_keywords : dict
        Dictionary of article keywords where the key is the article UID and the value is the list of keywords.
    merge_suggestions : dict
        Dictionary of keyword merge suggestions where the key is the merged keyword and the value is the list of original keywords.

    Returns
    -------
    dict
        Dictionary of updated article keywords where the key is the article UID and the value is the list of keywords.
    """
    if not merge_suggestions:
        logging.info("No merge suggestions provided or file is empty. Skipping merge" " process.")
        return article_keywords

    # Create a dictionary mapping original keywords to their merged counterparts
    merged_keywords = {
        original_keyword: merged_keyword
        for merged_keyword, original_keywords in merge_suggestions.items()
        for original_keyword in original_keywords
    }

    # Update article keywords using dictionary comprehension and set intersection
    updated_article_keywords = {
        article_id: list({merged_keywords.get(keyword, keyword) for keyword in keywords})
        for article_id, keywords in article_keywords.items()
    }

    return updated_article_keywords


def extract_topics(clusters: Dict[int, List[str]], article_keywords: Dict[str, List[str]]) -> Dict[int, Dict]:
    """
    Extract topics from clusters using the article keywords.

    Parameters
    ----------
    clusters : dict
        Dictionary of clusters where the key is the cluster ID and the value is the list of article UIDs.
    article_keywords : dict
        Dictionary of article keywords where the key is the article UID and the value is the list of keywords.

    Returns
    -------
    dict
        Dictionary of cluster topics where the key is the cluster ID and the value is a dictionary with the keywords and topic summary.
    """
    api_key = SecretStr(OPENAI_API_KEY) if OPENAI_API_KEY else None
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=api_key)

    topic_template = """Based on the following keywords extracted from multiple research articles:

    {context}

    Provide a brief summary (2-3 sentences) of the main research topic or theme these keywords from these articles represent:
    Summary:"""
    topic_prompt = PromptTemplate.from_template(topic_template)
    topic_chain = LLMChain(llm=llm, prompt=topic_prompt)

    results = {}
    for cluster_id, article_uids in clusters.items():
        cluster_keywords = set()
        for uid in article_uids:
            cluster_keywords.update(article_keywords.get(uid, []))

        all_keywords = ", ".join(cluster_keywords)
        topic_summary = topic_chain.invoke({"context": all_keywords}).get("text", "").strip()

        results[cluster_id] = {
            "keywords": list(cluster_keywords),
            "topic_summary": topic_summary,
        }

    return results


def update_neo4j(
    article_keywords: Dict[str, List[str]],
    cluster_results: Dict[int, Dict],
    clusters: Dict[int, List[str]],
    clustering_algorithm: str,
):
    """
    Update Neo4j database with article keywords and cluster topics.

    Parameters
    ----------
    article_keywords : dict
        Dictionary of article keywords where the key is the article UID and the value is the list of keywords.
    cluster_results : dict
        Dictionary of cluster topics where the key is the cluster ID and the value is a dictionary with the keywords and topic summary.
    clusters : dict
        Dictionary of clusters where the key is the cluster ID and the value is the list of article UIDs.
    clustering_algorithm : str
        Clustering algorithm used to generate the clusters.

    Returns
    -------
    None
    """
    # convert clustering_algorithm AgglomerativeClustering to AGGLOMERATIVE
    clustering_algorithm = clustering_algorithm.split("Clustering")[0].upper()

    with driver.session() as session:
        # Delete existing Keyword and Topic nodes and their relationships
        session.run(
            """
            MATCH (k:Keyword)
            DETACH DELETE k
            """
        )
        session.run(
            """
            MATCH (t:Topic)
            DETACH DELETE t
            """
        )
        logging.info("Existing Keyword and Topic nodes and their relationships deleted.")

        # Delete existing topic_summary and keywords properties on Cluster nodes
        session.run(
            """
            MATCH (c:Cluster)
            REMOVE c.topic_summary, c.keywords
            """
        )
        logging.info("Existing topic_summary and keywords properties removed from" " Cluster nodes.")

        # Update article keywords
        for article_id, keywords in article_keywords.items():

            # Create new Keyword nodes and relationships
            for keyword in keywords:
                session.run(
                    """
                    MERGE (k:Keyword {name: $keyword})
                    WITH k
                    MATCH (a:Article {uid: $article_id})
                    MERGE (a)-[:HAS_KEYWORD]->(k)
                    """,
                    article_id=article_id,
                    keyword=keyword,
                )

            # Update keywords property on Article node
            session.run(
                """
                MATCH (a:Article {uid: $article_id})
                SET a.keywords = $keywords
                """,
                article_id=article_id,
                keywords=keywords,
            )
        logging.info("Article keywords updated in Neo4j.")

        # Update cluster topics
        for cluster_id, data in cluster_results.items():
            topic_summary = data["topic_summary"]
            keywords = data["keywords"]
            # Set topic summary and keywords in Cluster node using the algorithm
            session.run(
                """
                MATCH (c:Cluster {cluster_id: $cluster_id, algorithm: $clustering_algorithm})
                SET c.topic_summary = $topic_summary,
                    c.keywords = $keywords
                """,
                cluster_id=int(cluster_id),
                topic_summary=topic_summary,
                keywords=keywords,
                clustering_algorithm=clustering_algorithm,
            )

        logging.info("Cluster topics are updated in Neo4j.")


def main():
    """Process keywords and extract topics."""
    with open(args.article_keywords, "r") as f:
        article_keywords = json.load(f)

    with open(args.clusters, "r") as f:
        cluster_data = json.load(f)

    clusters, clustering_algorithm = (
        cluster_data["clusters"],
        cluster_data["algorithm"],
    )

    if args.merge_suggestions and not args.skip_merge:
        merge_suggestions = load_merge_suggestions(args.merge_suggestions)
        if merge_suggestions:
            logging.info("Applying merge suggestions...")
            article_keywords = apply_merge_suggestions(article_keywords, merge_suggestions)
        else:
            logging.info("No merge suggestions found or file is empty. Proceeding with" " original keywords.")
    else:
        logging.info("Skipping merge process as requested or no merge suggestions file" " provided.")

    # check if cluster_results exists
    if os.path.exists("data/cluster_results.json") and not args.force_run:
        logging.info("Loading existing cluster results...")
        with open("data/cluster_results.json", "r") as f:
            cluster_results = json.load(f)
    else:
        cluster_results = extract_topics(clusters, article_keywords)

    logging.info("Updating Neo4j database...")
    update_neo4j(article_keywords, cluster_results, clusters, clustering_algorithm)

    # Save updated article keywords
    logging.info("Saving updated article keywords...")
    with open("data/updated_article_keywords.json", "w") as f:
        json.dump(article_keywords, f, indent=4)
    logging.info("Updated article keywords saved to data/updated_article_keywords.json")

    # Save cluster results
    with open("data/cluster_results.json", "w") as f:
        json.dump(cluster_results, f, indent=4)
    logging.info("Cluster results saved to data/cluster_results.json")


if __name__ == "__main__":
    main()
