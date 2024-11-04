"""Asyncronously extract keywords from articles and suggest merging similar keywords."""

import argparse
import asyncio
import json
import logging
import os
from typing import Dict, List

from dotenv import load_dotenv
from langchain.chains import LLMChain
from langchain.docstore.document import Document
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from neo4j import GraphDatabase
from pydantic import SecretStr
from tenacity import (  # for exponential backoff
    retry,
    stop_after_attempt,
    wait_random_exponential,
)

from citations.schemas import ClusterAnalysis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()


class Config:
    """Configuration settings."""

    NEO4J_URI: str = (
        os.getenv("NEO4J_URI", "bolt://localhost:7687")
        or "bolt://localhost:7687"
    )
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j") or "neo4j"
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "password") or "password"
    OPENAI_API_KEY: SecretStr = SecretStr(
        os.getenv("OPENAI_API_KEY") or "your_default_api_key"
    )
    DEFAULT_NUM_KEYWORDS: int = 3


config = Config()

if config.OPENAI_API_KEY is None:
    raise ValueError("OPENAI_API_KEY must be set in the configuration.")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate keywords and merge suggestions"
    )
    parser.add_argument(
        "--json-path",
        type=str,
        required=True,
        help="Path to JSON file with precomputed clusters",
    )
    parser.add_argument(
        "--num-keywords",
        type=int,
        default=config.DEFAULT_NUM_KEYWORDS,
        help="Number of keywords to extract per article",
    )
    parser.add_argument(
        "--force-reextract",
        action="store_true",
        help="Force re-extraction of keywords",
    )
    parser.add_argument(
        "--force-suggest",
        action="store_true",
        help="Force suggestions of merging keywords",
    )
    return parser.parse_args()


def load_clusters_from_json(json_path: str) -> Dict[int, List[str]]:
    """
    Load clusters from a JSON file.

    Parameters
    ----------
    json_path
        Path to JSON file.

    Returns
    -------
    dict
        Dictionary of clusters where the key is the cluster ID and the value is the list of article UIDs.
    """
    try:
        with open(json_path, "r") as f:
            data = json.load(f)
            cluster_analysis = ClusterAnalysis(**data)
            return cluster_analysis.clusters
    except FileNotFoundError:
        logger.error(f"File not found: {json_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {json_path}")
        raise


@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
async def extract_keywords_for_article(
    text: str,
    uid: str,
    keyword_chain: LLMChain,
    num_keywords: int,
) -> Dict[str, List[str]]:
    """
    Extract keywords from an article.

    Parameters
    ----------
    text
        Text of the article.
    uid
        UID of the article.
    keyword_chain
        LLMChain for extracting keywords.
    num_keywords
        Number of keywords to extract.

    Returns
    -------
    dict
        Dictionary with the UID of the article as the key and a list of keywords as the value.
    """
    document = Document(page_content=text)
    keywords = (
        (await keyword_chain.ainvoke(document))
        .get("text", "")
        .strip()
        .split(", ")
    )
    return {uid: keywords[:num_keywords]}


async def extract_keywords(
    clusters: Dict[int, List[str]], num_keywords: int, args
) -> Dict[str, List[str]]:
    """
    Extract keywords from articles in clusters.

    Parameters
    ----------
    clusters
        Dictionary of clusters where the key is the cluster ID and the value is the list of article UIDs.
    num_keywords
        Number of keywords to extract per article.
    args
        Command line arguments.

    Returns
    -------
    dict
        Dictionary with the UID of the article as the key and a list of keywords as the value.
    """
    llm = ChatOpenAI(
        model="gpt-4o-mini",
        temperature=0,
        api_key=config.OPENAI_API_KEY,
    )
    keyword_template = (
        f"Extract exactly {num_keywords} keywords from the following text,"
        " separated by commas:\n{context}\nKeywords:"
    )
    keyword_prompt = PromptTemplate.from_template(keyword_template)
    keyword_chain = LLMChain(llm=llm, prompt=keyword_prompt)

    article_keywords = {}
    if os.path.exists("data/article_keywords.json"):
        with open("data/article_keywords.json", "r") as f:
            article_keywords = json.load(f)

    driver = GraphDatabase.driver(
        config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD)
    )

    async def process_cluster(cluster_id: int, article_uids: List[str]):
        """
        Process articles in a cluster.

        Parameters
        ----------
        cluster_id
            Cluster ID.
        article_uids
            List of article UIDs in the cluster.

        Returns
        -------
        None
        """
        tasks = []
        with driver.session() as session:
            for uid in article_uids:
                if uid in article_keywords and not args.force_reextract:
                    continue
                logger.info(f"Extracting keywords for article {uid}")
                neo4j_result = session.run(
                    "MATCH (a:Article {uid: $uid}) RETURN a.title + ' ' +"
                    " COALESCE(a.abstract, '') AS text, a.uid AS uid",
                    uid=uid,
                )
                record = neo4j_result.single()
                if record:
                    text, uid = record["text"], record["uid"]
                    task = extract_keywords_for_article(
                        text, uid, keyword_chain, num_keywords
                    )
                    tasks.append(task)

        results = await asyncio.gather(*tasks)
        for result in results:
            article_keywords.update(result)

    await asyncio.gather(
        *[
            process_cluster(cluster_id, article_uids)
            for cluster_id, article_uids in clusters.items()
        ]
    )
    driver.close()

    return article_keywords


@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
async def generate_merge_suggestions(
    keywords: List[str], llm: ChatOpenAI
) -> List[Dict[str, List[str]]]:
    """
    Generate suggestions for merging similar keywords.

    Parameters
    ----------
    keywords
        List of keywords.
    llm
        ChatOpenAI instance.

    Returns
    -------
    list
        List of dictionaries with the merged keyword as the key and a list of existing keywords as the value.
    """
    merge_template = """
    Given the following list of keywords:
    {keywords}

    Identify any similar or duplicate keywords and suggest merging them.
    You can suggest merging more than two keywords if appropriate.
    Provide your suggestions in the following format:
    1. merged_keyword_1: [existing_keyword_1, existing_keyword_2, existing_keyword_3]
    2. merged_keyword_2: [existing_keyword_a, existing_keyword_b]
    ...

    Example:
    1. neuroscience: [brain science, neurology]
    2. machine learning: [ML, artificial intelligence, quantum machine learning]

    Find as much suggestions as possible. Only suggest merging keywords that actually appear in the list.
    Suggestions:
    """
    merge_prompt = PromptTemplate.from_template(merge_template)
    merge_chain = LLMChain(llm=llm, prompt=merge_prompt)

    merge_suggestions = (
        (await merge_chain.ainvoke({"keywords": ", ".join(keywords)}))
        .get("text", "")
        .strip()
        .split("\n")
    )

    suggestions = []
    for suggestion in merge_suggestions:
        if not suggestion.strip():
            continue
        parts = suggestion.split(": ")
        if len(parts) != 2:
            logger.warning(f"Unexpected format in suggestion: {suggestion}")
            continue
        merged_keyword = parts[0].split(". ")[-1].strip()
        existing_keywords = [
            kw.strip() for kw in parts[1].strip("[]").split(",")
        ]
        suggestions.append({merged_keyword: existing_keywords})

    return suggestions


async def main():
    """Run main function."""
    args = parse_arguments()
    all_clusters = load_clusters_from_json(args.json_path)

    if os.path.exists("data/article_keywords.json"):
        logger.info("Extracting Keywords from Articles")
        article_keywords = await extract_keywords(
            all_clusters, args.num_keywords, args
        )
        with open("data/article_keywords.json", "w") as f:
            json.dump(article_keywords, f, indent=4)
        logger.info("Article keywords saved to data/article_keywords.json")

    if args.force_suggest or not os.path.exists(
        "data/keyword_merge_suggestions.jsonl"
    ):
        with open("data/article_keywords.json", "r") as f:
            article_keywords = json.load(f)
        logger.info("Generating suggestions for merging similar keywords.")
        all_keywords = {
            keyword
            for keywords in article_keywords.values()
            for keyword in keywords
        }
        merge_suggestions = await generate_merge_suggestions(
            all_keywords,
            ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0,
                openai_api_key=config.OPENAI_API_KEY,
            ),
        )
        with open("data/keyword_merge_suggestions.jsonl", "w") as f:
            for suggestion in merge_suggestions:
                f.write(json.dumps(suggestion) + "\n")
        logger.info(
            "Suggestions saved to data/keyword_merge_suggestions.jsonl"
        )


if __name__ == "__main__":
    asyncio.run(main())
