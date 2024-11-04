#!/usr/bin/env python3
"""
SERP Data Processor.

This script processes a JSONL file containing SERP API data, merges it with an existing database,
and generates four CSV files: articles_serp.csv, authors_serp.csv,
author_wrote_article_serp.csv, and article_cites_article_serp.csv.

Usage:
    python serp_data_processor.py --input <input_file> --output <output_directory> --data_dir <data_dir>

Author: Kerem Kurban
Date: 24.08.2024
"""

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from rapidfuzz import fuzz, process

from citations.schemas import Article, ArticleCitesArticle, AuthorWroteArticle

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def read_jsonl(file_path: str) -> List[Dict[str, Any]]:
    """Read JSONL file and return a list of dictionaries."""
    with open(file_path, "r", encoding="utf-8") as file:
        return [json.loads(line) for line in file]


def normalize_title(title: str) -> str:
    """Normalize the title by removing special characters and converting to lowercase."""
    return re.sub(r"[^\w\s]", "", title.lower())


def normalize_author_name(name: str) -> str:
    """Normalize author name by removing special characters and converting to lowercase."""
    if pd.isna(name):
        return ""
    return re.sub(r"[^\w\s]", "", str(name).lower()).strip()


def get_initials(name: str) -> str:
    """Extract initials from a name."""
    return "".join(word[0] for word in name.split() if word)


def get_last_name(name: str) -> str:
    """Get last name from full name."""
    parts = name.split()
    return parts[-1] if parts else ""


def process_authors(data: List[Dict[str, Any]], existing_db: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Process authors from SERP data and merge with existing database.

    Args:
    ----
        data (List[Dict[str, Any]]): List of dictionaries containing author data.
        existing_db (pd.DataFrame): DataFrame of existing authors.

    Returns:
    -------
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the merged DataFrame and new entries.
    """
    authors = set()
    for item in data:
        for citation in item.get("citations", []):
            for author in citation.get("authors", []):
                authors.add((author.get("author_id", ""), author.get("name", "")))

    authors_df = pd.DataFrame(authors, columns=["google_scholar_id", "name"])
    logging.info(f"Total new authors: {len(authors_df)}")
    logging.info(f"Total existing authors: {len(existing_db)}")

    # Normalize author names
    authors_df["normalized_name"] = authors_df["name"].apply(normalize_author_name)
    existing_db["normalized_name"] = existing_db["name"].apply(normalize_author_name)

    # Create additional columns for blocking
    authors_df["initials"] = authors_df["normalized_name"].apply(get_initials)
    authors_df["last_name"] = authors_df["normalized_name"].apply(get_last_name)
    existing_db["initials"] = existing_db["normalized_name"].apply(get_initials)
    existing_db["last_name"] = existing_db["normalized_name"].apply(get_last_name)

    # Create a set to keep track of matched existing authors
    matched_existing_authors = set()

    # Perform blocking and matching
    merged_rows = []
    match_count = 0
    for _, new_author in authors_df.iterrows():
        # Filter existing_db based on initials and last name
        potential_matches = existing_db[
            (existing_db["initials"] == new_author["initials"]) | (existing_db["last_name"] == new_author["last_name"])
        ]

        matched = False
        if not potential_matches.empty:
            # Use rapidfuzz for faster string matching
            matches = process.extractOne(
                new_author["normalized_name"],
                potential_matches["normalized_name"].tolist(),
                scorer=fuzz.ratio,
                score_cutoff=90,  # Lowered the score cutoff
            )

            if matches:
                match_name, match_score, match_index = matches
                existing_author = potential_matches.iloc[match_index]
                merged_row = {
                    "uid": existing_author["uid"],
                    "orcid_id": existing_author["orcid_id"],
                    "name": existing_author["name"],
                    "google_scholar_id": new_author["google_scholar_id"] or existing_author.get("google_scholar_id"),
                }
                merged_rows.append(merged_row)
                matched_existing_authors.add(existing_author["uid"])
                logging.debug(
                    f"Matched author: {new_author['name']} with" f" {existing_author['name']} (score: {match_score})"
                )
                matched = True
                match_count += 1

        if not matched:
            merged_rows.append(
                {
                    "uid": new_author["google_scholar_id"],
                    "orcid_id": None,
                    "name": new_author["name"],
                    "google_scholar_id": new_author["google_scholar_id"],
                }
            )

    logging.info(f"Total matched authors: {match_count}")

    # Add unmatched existing authors to merged_rows
    unmatched_existing = 0
    for _, existing_author in existing_db.iterrows():
        if existing_author["uid"] not in matched_existing_authors:
            merged_rows.append(
                {
                    "uid": existing_author["uid"],
                    "orcid_id": existing_author["orcid_id"],
                    "name": existing_author["name"],
                    "google_scholar_id": existing_author.get("google_scholar_id"),
                }
            )
            unmatched_existing += 1

    logging.info(f"Unmatched existing authors: {unmatched_existing}")

    merged_df = pd.DataFrame(merged_rows)
    merged_df = merged_df.drop_duplicates(subset="uid", keep="first")
    logging.info(f"Total merged authors: {len(merged_df)}")

    # Identify new entries (those without an orcid_id)
    new_entries = merged_df[merged_df["orcid_id"].isna()]
    new_entries = new_entries.drop_duplicates(subset="uid", keep="first")

    logging.info(f"Number of new authors from serp: {len(new_entries)}")

    return merged_df, new_entries


def process_articles(data: List[Dict[str, Any]], existing_db: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process articles from SERP data and merge with existing database.

    Args:
    ----
        data (List[Dict[str, Any]]): List of dictionaries containing article data.
        existing_db (pd.DataFrame): DataFrame of existing articles.

    Returns:
    -------
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the merged DataFrame and new entries.
    """
    processed_google_scholar_ids = set()
    articles = []
    for item in data:
        article_data = Article(
            uid=item.get("article_id", ""),
            title=item.get("title", ""),
            source="serp",
            is_bbp=True,
            is_published=True,
            publication_date=None,
            abstract=None,
            doi=None,
            pmid=None,
            europmc_id=None,
            google_scholar_id=item.get("article_id", ""),
            url=item.get("url"),
            isbns=None,
            citations=item.get("total_citations"),
        )
        articles.append(article_data)
        processed_google_scholar_ids.add(item.get("article_id", ""))

        for citation in item.get("citations", []):
            if citation.get("result_id") in processed_google_scholar_ids:
                continue

            citation_data = Article(
                uid=citation.get("result_id", ""),
                title=citation.get("title", ""),
                source="serp",
                is_bbp=False,
                is_published=True,
                publication_date=None,
                abstract=None,
                doi=None,
                pmid=None,
                europmc_id=None,
                google_scholar_id=citation.get("result_id", ""),
                url=citation.get("link"),
                isbns=None,
                citations=citation.get("cited_by", 0),
            )
            articles.append(citation_data)
            processed_google_scholar_ids.add(citation.get("result_id", ""))

    articles_df = pd.DataFrame([article.dict() for article in articles])
    articles_df["normalized_title"] = articles_df["title"].apply(normalize_title)
    # drop duplicates based on normalized title
    articles_df = articles_df.drop_duplicates(subset="normalized_title")

    existing_db["normalized_title"] = existing_db["title"].apply(normalize_title)

    # Merge dataframes without adding suffixes to uid and google_scholar_id
    merged_df = pd.merge(
        articles_df,
        existing_db,
        on="normalized_title",
        how="outer",
        suffixes=("_serp", "_existing"),
    )

    num_multiple_citation_count = 0
    num_no_citation_count = 0

    for index, row in merged_df.iterrows():
        # Combine sources
        if pd.notna(row.get("source_serp")) and pd.notna(row.get("source_existing")):
            merged_df.at[index, "source"] = f"serp_{row['source_existing']}"
        elif pd.notna(row.get("source_serp")):
            merged_df.at[index, "source"] = row["source_serp"]
        else:
            merged_df.at[index, "source"] = row.get("source_existing")

        # get title from existing db, if available. if not get from serp
        if pd.notna(row.get("title_existing")):
            merged_df.at[index, "title"] = row["title_existing"].strip()
        elif pd.notna(row.get("title_serp")):
            processed_title = row["title_serp"].strip().lower().replace('"', "").replace("'", "").strip()
            merged_df.at[index, "title"] = processed_title
        else:
            logging.error(f"Title is missing for article {row.get('uid')}")

        # Get max of available citation count information
        if pd.notna(row.get("citations_existing")) and pd.notna(row.get("citations_serp")):
            num_multiple_citation_count += 1
            # find the max citation count
            merged_df.at[index, "citations"] = max(row["citations_existing"], row["citations_serp"])
        elif pd.notna(row.get("citations_serp")):
            merged_df.at[index, "citations"] = row["citations_serp"]
        elif pd.notna(row.get("citations_existing")):
            merged_df.at[index, "citations"] = row["citations_existing"]
        else:
            num_no_citation_count += 1
            merged_df.at[index, "citations"] = 0

        # Get all other fields from existing db using the pydantic Article schema if we haven't already.
        # for new articles where existing data doesn't exist, leave it as None
        columns_from_existing = [
            "is_bbp",
            "is_published",
            "publication_date",
            "url",
            "abstract",
            "doi",
            "pmid",
            "europmc_id",
            "isbns",
        ]
        for field in columns_from_existing:
            existing_field = f"{field}_existing"
            new_field = f"{field}_serp"
            if field in merged_df.columns:
                if pd.notna(merged_df.at[index, field]):
                    continue
            try:
                if pd.notna(row.get(existing_field)):
                    merged_df.at[index, field] = row[existing_field]
                elif pd.notna(row.get(new_field)):
                    merged_df.at[index, field] = row[new_field]
            except Exception as e:
                logging.error(f"Error in fetching {field}: {str(e)}")

    logging.info(
        f"{num_multiple_citation_count} articles had citation information from" " multiple sources. Took the max value."
    )
    logging.info(f"{num_no_citation_count} articles had no citation information. (value" " set to 0)")

    # Remove temporary columns and normalize the DataFrame
    columns_to_keep = list(Article.model_fields.keys())
    columns_to_keep.append("normalized_title")  # Keep normalized_title for now

    merged_df["uid"] = merged_df["uid_existing"].fillna(merged_df["uid_serp"])
    # Select only the columns we want to keep
    final_df = merged_df[columns_to_keep]

    # Identify new entries
    new_entries = final_df[final_df["uid"].isin(articles_df["uid"]) & ~final_df["uid"].isin(existing_db["uid"])]

    # Remove normalized_title from final dataframes
    final_df = final_df.drop(columns=["normalized_title"])
    final_df = final_df.drop_duplicates(subset="uid", keep="first")

    new_entries = new_entries.drop(columns=["normalized_title"])
    new_entries = new_entries.drop_duplicates(subset="uid", keep="first")

    return final_df, new_entries


def process_author_wrote_article(
    data: List[Dict[str, Any]],
    existing_db: pd.DataFrame,
    authors_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process the relationship between authors and articles from SERP data and merge with an existing database.

    Args:
    ----
        data (List[Dict[str, Any]]): A list of dictionaries containing article and author data.
        existing_db (pd.DataFrame): A DataFrame of existing author-article relationships.
        authors_df (pd.DataFrame): A DataFrame of authors.

    Returns:
    -------
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the merged DataFrame of author-article relationships
        and a DataFrame of new entries that are not present in the existing database.
    """
    author_wrote_article = []

    # Process main articles
    for item in data:
        article_id = item.get("article_id", "error_in_cited_article_google_id")
        if "authors" in item:
            for author in item["authors"]:
                awa = AuthorWroteArticle(
                    author_uid=author.get("author_id", "error_in_cited_author_google_id"),
                    article_uid=article_id,
                )
                author_wrote_article.append(awa)

        # Process citations
        for citation in item.get("citations", []):
            citation_id = citation.get("result_id", "")
            for author in citation.get("authors", []):
                awa = AuthorWroteArticle(
                    author_uid=author.get("author_id", "error_in_citing_author_google_id"),
                    article_uid=citation_id,
                )
                author_wrote_article.append(awa)

    awa_df = pd.DataFrame([awa.dict() for awa in author_wrote_article])

    # Merge with existing database
    merged_df = pd.merge(
        awa_df,
        existing_db,
        on=["author_uid", "article_uid"],
        how="outer",
        indicator=True,
    )

    # Identify new entries
    new_entries = merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])
    # Prepare final DataFrame
    final_df = merged_df.drop(columns=["_merge"])

    final_df = final_df.drop_duplicates(subset=["author_uid", "article_uid"], keep="first")
    new_entries = new_entries.drop_duplicates(subset=["author_uid", "article_uid"], keep="first")

    logging.info(f"Total author_wrote_article entries: {len(final_df)}")
    logging.info(f"New author_wrote_article entries: {len(new_entries)}")

    return final_df, new_entries


def process_article_cites_article(
    data: List[Dict[str, Any]], existing_db: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Process article citations from SERP data and merge with existing database.

    Args:
    ----
        data (List[Dict[str, Any]]): List of dictionaries containing citation data.
        existing_db (pd.DataFrame): DataFrame of existing article citations.

    Returns:
    -------
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the merged DataFrame and new entries.
    """
    article_cites_article = []
    for item in data:
        for citation in item.get("citations", []):
            aca = ArticleCitesArticle(
                article_uid_source=citation.get("result_id", ""),
                article_uid_target=item.get("article_id", ""),
            )
            article_cites_article.append(aca)

    aca_df = pd.DataFrame([aca.dict() for aca in article_cites_article])

    # Merge with existing database
    merged_df = pd.merge(
        aca_df,
        existing_db,
        on=["article_uid_source", "article_uid_target"],
        how="outer",
        indicator=True,
    )

    # Identify new entries
    new_entries = merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])

    # Prepare final DataFrame
    final_df = merged_df.drop(columns=["_merge"])
    final_df = final_df.drop_duplicates(subset=["article_uid_source", "article_uid_target"], keep="first")

    new_entries = new_entries.drop_duplicates(subset=["article_uid_source", "article_uid_target"], keep="first")

    logging.info(f"Total article_cites_article entries: {len(final_df)}")
    logging.info(f"New article_cites_article entries: {len(new_entries)}")

    return final_df, new_entries


def main(input_file: str, output_dir: str, data_dir: str) -> None:
    """Process JSONL, merge with existing database, and create CSV files."""
    data = read_jsonl(input_file)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    suffix = "_before_serp"
    existing_articles_dir = Path(data_dir) / f"articles{suffix}.csv"
    existing_authors_dir = Path(data_dir) / f"authors{suffix}.csv"
    existing_author_wrote_article_dir = Path(data_dir) / f"author_wrote_article{suffix}.csv"
    existing_article_cites_article_dir = Path(data_dir) / f"article_cites_article{suffix}.csv"

    # Load existing database
    existing_articles = pd.read_csv(existing_articles_dir)
    existing_authors = pd.read_csv(existing_authors_dir)
    existing_author_wrote_article = pd.read_csv(existing_author_wrote_article_dir, index_col=0)
    existing_article_cites_article = pd.read_csv(existing_article_cites_article_dir)

    # Process and write articles_serp.csv
    articles, serp_article_additions = process_articles(data, existing_articles)
    # breakpoint()
    articles.to_csv(output_path / "articles.csv", index=False)
    serp_article_additions.to_csv(output_path / "serp_article_additions.csv", index=False)

    # Process and write authors_serp.csv
    authors, serp_author_additions = process_authors(data, existing_authors)
    authors.to_csv(output_path / "authors.csv", index=False)
    serp_author_additions.to_csv(output_path / "serp_author_additions.csv", index=False)

    # breakpoint()
    # Process and write author_wrote_article_serp.csv
    author_wrote_article, serp_author_wrote_article_additions = process_author_wrote_article(
        data, existing_author_wrote_article, authors
    )
    # breakpoint()
    author_wrote_article.to_csv(output_path / "author_wrote_article.csv", index=False)
    serp_author_wrote_article_additions.to_csv(output_path / "serp_author_wrote_article_additions.csv", index=False)

    # Process and write article_cites_article_serp.csv
    article_cites_article, serp_article_cites_article_additions = process_article_cites_article(
        data, existing_article_cites_article
    )
    article_cites_article.to_csv(output_path / "article_cites_article.csv", index=False)
    serp_article_cites_article_additions.to_csv(output_path / "serp_article_cites_article_additions.csv", index=False)

    print("\nSummary\n\nTotal number of articles:" f" {len(articles)} ({len(serp_article_additions)} new)")
    # print(f"Number of new articles from SERP api: {len(serp_article_additions)}")
    print("Total number of authors:" f" {len(authors)} ({len(serp_author_additions)} new)")
    # print(f"Number of new authors from SERP api: {len(serp_author_additions)}")
    print(
        "Total number of author_wrote_article:"
        f" {len(author_wrote_article)} ({len(serp_author_wrote_article_additions)} new)"
    )
    # print(
    #     f"Number of new author_wrote_article edges from SERP api: {len(serp_author_wrote_article_additions)}"
    # )
    print(
        "Total number of article_cites_article:"
        f" {
            len(article_cites_article)} ({
            len(serp_article_cites_article_additions)} new)"
    )
    # print(
    #     f"Number of new article_cites_article from SERP api: {len(serp_article_cites_article_additions)}"
    # )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Process SERP API data from JSONL to CSV files and merge with" " existing database.")
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the SERP citation results JSONL file",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Directory to save the output CSV files",
    )
    parser.add_argument(
        "--data_dir",
        required=True,
        help="Path to the data dir containing existing database CSV files",
    )
    args = parser.parse_args()

    main(args.input, args.output, args.data_dir)
