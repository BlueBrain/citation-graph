#!/usr/bin/env python3
"""
SERP Data Processor.

This script processes a JSONL file containing SERP API data and generates
four CSV files: articles_serp.csv, authors_serp.csv,
author_wrote_article_serp.csv, and article_cites_article_serp.csv.

Usage:
    python serp_data_processor.py --input <input_file> --output <output_directory>

Author: Kerem Kurban
Date: 24.08.2024
"""

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def read_jsonl(file_path: str) -> List[Dict[str, Any]]:
    """
    Read JSONL file and return a list of dictionaries.

    Args:
    ----
        file_path (str): Path to the input JSONL file.

    Returns:
    -------
        List[Dict[str, Any]]: List of dictionaries containing the JSONL data.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return [json.loads(line) for line in file]


def write_csv(
    file_path: str, headers: List[str], data: List[List[Any]]
) -> None:
    """
    Write data to a CSV file.

    Args:
    ----
        file_path (str): Path to the output CSV file.
        headers (List[str]): List of column headers.
        data (List[List[Any]]): List of rows to write to the CSV.
    """
    with open(file_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)


def process_articles(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Process articles data for articles_serp.csv.

    Args:
    ----
        data (List[Dict[str, Any]]): List of dictionaries containing JSONL data.

    Returns:
    -------
        pd.DataFrame: Processed data for articles_serp.csv.
    """
    total_bbp_citations = 0
    total_bbp_articles = 0
    articles = []
    for item in data:
        articles.append(
            [
                item["article_id"],
                item["total_citations"],
                item["title"],
            ]
        )
        total_bbp_citations += item["total_citations"]
        total_bbp_articles += 1
        for citation in item.get("citations", []):
            articles.append(
                [
                    citation["result_id"],
                    citation.get("cited_by", 0),
                    citation.get("title", ""),
                    citation.get("link", ""),
                    # Add other relevant fields here
                ]
            )
    # convert to pandas df and drop duplicates based on article_id and result_id
    articles_df = pd.DataFrame(
        articles, columns=["article_id", "citations", "title", "link"]
    )
    articles_df = articles_df.drop_duplicates(subset=["article_id", "title"])

    print(
        f"Total BBP citations: {total_bbp_citations} out of"
        f" {total_bbp_articles} articles"
    )
    return articles_df


def process_authors(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Process authors data for authors_serp.csv.

    Args:
    ----
        data (List[Dict[str, Any]]): List of dictionaries containing JSONL data.

    Returns:
    -------
        pd.DataFrame: Processed data for authors_serp.csv.
    """
    authors = set()
    for item in data:
        for citation in item.get("citations", []):
            for author in citation.get("authors", []):
                authors.add((author["author_id"], author["name"]))

    # convert to pandas df and drop duplicates based on author_id
    authors_df = pd.DataFrame(authors, columns=["author_id", "name"])
    authors_df = authors_df.drop_duplicates(subset=["author_id"])

    return authors_df


def process_author_wrote_article(
    data: List[Dict[str, Any]],
) -> pd.DataFrame:
    """
    Process data for author_wrote_article_serp.csv.

    Args:
    ----
        data (List[Dict[str, Any]]): List of dictionaries containing JSONL data.

    Returns:
    -------
        pd.DataFrame: Processed data for author_wrote_article_serp.csv.
    """
    author_wrote_article = []
    for item in data:
        for citation in item.get("citations", []):
            for author in citation.get("authors", []):
                author_wrote_article.append(
                    [author["author_id"], citation["result_id"]]
                )

    # convert to pandas df and drop duplicates based on author_id,article_id pair
    author_wrote_article_df = pd.DataFrame(
        author_wrote_article, columns=["author_id", "article_id"]
    )
    author_wrote_article_df = author_wrote_article_df.drop_duplicates(
        subset=["author_id", "article_id"]
    )

    return author_wrote_article_df


def process_article_cites_article(
    data: List[Dict[str, Any]],
) -> pd.DataFrame:
    """
    Process data for article_cites_article_serp.csv.

    Args:
    ----
        data (List[Dict[str, Any]]): List of dictionaries containing JSONL data.

    Returns:
    -------
        pd.DataFrame: Processed data for article_cites_article_serp.csv.
    """
    article_cites_article = []
    for item in data:
        for citation in item.get("citations", []):
            article_cites_article.append(
                [citation["result_id"], item["article_id"]]
            )

    # convert to pandas df and drop duplicates based on source and target
    article_cites_article_df = pd.DataFrame(
        article_cites_article, columns=["source", "target"]
    )
    article_cites_article_df = article_cites_article_df.drop_duplicates(
        subset=["source", "target"]
    )

    return article_cites_article_df


def main(input_file: str, output_dir: str) -> None:
    """
    Process JSONL and create CSV files.

    Args:
    ----
        input_file (str): Path to the input JSONL file containing article and author info from SERP
        output_dir (str): Directory to save the output CSV files.
    """
    data = read_jsonl(input_file)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Process and write articles_serp.csv
    articles = process_articles(data)
    articles.to_csv(output_path / "articles_serp.csv", index=False)

    # Process and write authors_serp.csv
    authors = process_authors(data)
    authors.to_csv(output_path / "authors_serp.csv", index=False)

    # Process and write author_wrote_article_serp.csv
    author_wrote_article = process_author_wrote_article(data)
    author_wrote_article.to_csv(
        output_path / "author_wrote_article_serp.csv", index=False
    )

    # Process and write article_cites_article_serp.csv
    article_cites_article = process_article_cites_article(data)
    article_cites_article.to_csv(
        output_path / "article_cites_article_serp.csv", index=False
    )

    # log how many articles, authors, author_wrote_article, article_cites_article
    print(f"Number of articles: {len(articles)}")
    print(f"Number of authors: {len(authors)}")
    print(f"Number of author_wrote_article: {len(author_wrote_article)}")
    print(f"Number of article_cites_article: {len(article_cites_article)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process SERP API data from JSONL to CSV files."
    )
    parser.add_argument(
        "--input", required=True, help="Path to the input JSONL file"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Directory to save the output CSV files",
    )
    args = parser.parse_args()

    main(args.input, args.output)
