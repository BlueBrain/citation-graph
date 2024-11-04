"""Fetch citations for articles using the SerpApi and save results to a JSONL file."""

import argparse
import csv
import json
import logging
import os
import time
from typing import Dict, Optional

import requests
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class SerpApiCitationChecker:
    """
    A class to check citations for articles using the SerpApi.

    This class provides functionality to fetch article citations from the SerpApi
    and save the results to a specified output directory. It handles rate limiting
    and retries for API requests.

    Attributes:
    ----------
    api_key : str
        The API key for accessing the SerpApi.
    csv_file : str
        The path to the CSV file containing article information.
    output_dir : str
        The directory where the output JSONL file will be saved.
    base_url : str
        The base URL for the SerpApi.
    requests_made : int
        The number of API requests made.
    max_requests_per_hour : int
        The maximum number of API requests allowed per hour.

    Methods:
    -------
    make_api_request(params: Dict) -> Dict
        Makes a request to the SerpApi with the given parameters and returns the response data.
    get_article_id(row: Dict) -> Optional[str]
        Retrieves the article ID for a given row of article data.
    """

    def __init__(self, api_key: str, csv_file: str, output_dir: str):
        self.api_key = api_key
        self.csv_file = csv_file
        self.output_dir = output_dir
        self.base_url = "https://serpapi.com/search.json"
        self.requests_made = 0
        self.max_requests_per_hour = 1000
        os.makedirs(self.output_dir, exist_ok=True)

    @sleep_and_retry
    @limits(calls=1000, period=3600)
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def make_api_request(self, params: Dict) -> Dict:
        """
        Make a request to the SerpApi with the given parameters and returns the response data.

        This function handles the API request to the SerpApi using the provided parameters.
        It manages rate limiting and retries in case of failures.

        Args:
        ----
        params : Dict
            A dictionary containing the parameters for the API request.

        Returns:
        -------
        Dict
            A dictionary containing the response data from the SerpApi.

        Raises:
        ------
        HTTPError
            If the HTTP request returned an unsuccessful status code.
        Exception
            If there is an error during the API request.
        """
        self.requests_made += 1
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        return response.json()

    def get_article_id(self, row: Dict) -> Optional[str]:
        """
        Retrieve the article ID from a given row of article data.

        This function attempts to fetch the article ID by querying the SerpApi
        using various fields such as title, DOI, PMID, URL, and ISBNs. It handles
        exceptions and logs errors if the article ID cannot be retrieved.

        Args:
        ----
        row : Dict
            A dictionary containing article data with fields like title, DOI, PMID, URL, and ISBNs.

        Returns:
        -------
        Optional[str]
            The article ID if found, otherwise None.

        Raises:
        ------
        Exception
            If there is an error during the API request.
        """
        title = row.get("title", "")
        doi = row.get("doi", "")
        pmid = row.get("pmid", "")
        url = row.get("url", "")
        isbns = row.get("isbns", "")

        search_fields = [
            ("title", title),
            ("doi", doi),
            ("pmid", pmid),
            ("url", url),
            ("isbns", isbns),
        ]

        for field_name, field_value in search_fields:
            if not field_value:
                continue

            params = {
                "engine": "google_scholar",
                "q": field_value,
                "api_key": self.api_key,
            }
            try:
                data = self.make_api_request(params)
            except Exception as e:
                logging.error(
                    "Error fetching article ID for"
                    f" {field_name} '{field_value}': {str(e)}"
                )
                self.save_exception("get_article_id", "", field_value, str(e))
                continue

            for result in data.get("organic_results", []):
                if (
                    field_name == "title"
                    and result["title"].lower() == title.lower()
                ):
                    return result["result_id"]
                elif (
                    field_name in ["doi", "pmid", "url", "isbns"]
                    and title.lower() in result["title"].lower()
                ):
                    return result["result_id"]

        logging.warning(f"No article ID found for title: {title}")
        self.save_exception(
            "get_article_id",
            "",
            title,
            "No article ID returned for any field.",
        )
        return None

    def get_citations(self, article_id: str) -> Dict:
        """
        Fetch citations for a given article ID.

        This function retrieves citation data for a specified article
        using its unique article ID. It handles asynchronous operations
        to efficiently fetch data from the API.

        Args:
        ----
            article_id (str): The unique identifier for the article.

        Returns:
        -------
            Dict: A dictionary containing citation data for the article.
        """
        all_citations = []
        total_citations = 0
        start = 0

        try:
            while True:
                params = {
                    "engine": "google_scholar",
                    "cites": article_id,
                    "start": start,
                    "num": 20,
                    "api_key": self.api_key,
                    "hl": "en",
                }
                data = self.make_api_request(params)

                total_citations = int(
                    data["search_information"]["total_results"]
                )
                citations = data.get("organic_results", [])

                for citation in citations:
                    citation_info = {
                        "title": citation.get("title"),
                        "result_id": citation.get("result_id"),
                        "link": citation.get("link"),
                        "authors": [],
                        "cited_by": None,
                    }

                    # Handle authors
                    publication_info = citation.get("publication_info", {})
                    authors = publication_info.get("authors", [])
                    for author in authors:
                        author_info = {
                            "name": author.get("name"),
                            "author_id": author.get("author_id"),
                        }
                        citation_info["authors"].append(author_info)
                    if len(authors) == 0:
                        self.save_exception(
                            "get_citations",
                            citation.get("result_id"),
                            article_id,
                            "No authors found for citing article",
                        )

                    # Handle cited_by
                    inline_links = citation.get("inline_links", {})
                    cited_by = inline_links.get("cited_by", {})
                    if cited_by:
                        citation_info["cited_by"] = cited_by.get("total")

                    all_citations.append(citation_info)

                    # Log missing fields
                    missing_fields = [
                        field
                        for field, value in citation_info.items()
                        if value is None
                    ]
                    if missing_fields:
                        self.save_exception(
                            "get_citations",
                            citation.get("result_id"),
                            article_id,
                            f"Missing fields: {', '.join(missing_fields)}",
                        )

                if (
                    len(all_citations) >= total_citations
                    or len(citations) == 0
                ):
                    break

                start += 20
        except Exception as e:
            logging.error(
                f"Error fetching citations for article ID '{article_id}':"
                f" {str(e)}"
            )
            self.save_exception("get_citations", "", article_id, str(e))

        return {"total_citations": total_citations, "citations": all_citations}

    def save_exception(
        self,
        function: str,
        citing_article_identifier: str,
        cited_article_identifier: str,
        reason: str,
    ):
        """
        Save exception details to a JSONL file.

        This function logs exceptions encountered during the execution of
        various functions in the SerpApiCitationChecker class. It records
        the function name, identifiers for the citing and cited articles,
        the reason for the exception, and a timestamp.

        Args:
        ----
        function : str
            The name of the function where the exception occurred.
        citing_article_identifier : str
            The identifier of the citing article, if available.
        cited_article_identifier : str
            The identifier of the cited article, if available.
        reason : str
            A description of the reason for the exception.

        Returns:
        -------
        None
        """
        exception = {
            "function": function,
            "citing_article_identifier": citing_article_identifier,
            "cited_article_identifier": cited_article_identifier,
            "reason": reason,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        }
        with open(
            os.path.join(self.output_dir, "serp_api_exceptions.json"), "a"
        ) as f:
            json.dump(exception, f)
            f.write("\n")

    def process_row(self, row: Dict) -> Optional[Dict]:
        """
        Process a single row of article data.

        This function processes a row from the CSV file, checks if the article
        is a BBP and published, retrieves the article ID, and fetches its citations.
        If successful, it returns a dictionary containing the article's title, ID,
        total citations, and citation details.

        Args:
        ----
        row : Dict
            A dictionary representing a row of article data from the CSV file.

        Returns:
        -------
        Optional[Dict]
            A dictionary with the article's title, ID, total citations, and citation
            details if the article is a BBP and published; otherwise, None.
        """
        if row.get("is_bbp") == "True" and row.get("is_published") == "True":
            article_id = self.get_article_id(row)
            if article_id:
                citations = self.get_citations(article_id)
                return {
                    "title": row["title"],
                    "article_id": article_id,
                    "total_citations": citations["total_citations"],
                    "citations": citations["citations"],
                }
            else:
                logging.warning(
                    f"No article ID found for title: {row['title']}"
                )
                self.save_exception(
                    "get_article_id",
                    "",
                    row["title"],
                    "No article ID returned.",
                )

        return None

    def process_csv(self):
        """
        Process the CSV file to fetch citations for articles.

        This function reads a CSV file containing article data, processes each row to
        check if the article is a BBP and published, retrieves the article ID, and
        fetches its citations. The results are saved to a JSONL file in the specified
        output directory.

        Args:
        ----
        None

        Returns:
        -------
        List[Dict]: A list of dictionaries, each containing the article's title, ID,
                    total citations, and citation details.
        """
        results = []

        output_file = os.path.join(
            self.output_dir, "serp_citation_results.jsonl"
        )

        with (
            open(self.csv_file, "r") as file,
            open(output_file, "w") as outfile,
        ):
            reader = csv.DictReader(file)
            for row in reader:
                row_result = self.process_row(row)
                if row_result:
                    results.append(row_result)
                    json.dump(row_result, outfile)
                    outfile.write("\n")

                if self.requests_made >= self.max_requests_per_hour:
                    logging.info(
                        f"Reached {self.max_requests_per_hour} requests."
                        " Waiting for 1 hour..."
                    )
                    time.sleep(3600)
                    self.requests_made = 0

        logging.info(f"Results saved to {output_file}")
        return results


def get_parser():
    """
    Get the argument parser for the script.

    This function sets up and returns an argument parser for the script,
    which is used to parse command-line arguments.

    Returns:
    -------
    argparse.ArgumentParser
        The argument parser configured with the necessary arguments.
    """
    parser = argparse.ArgumentParser(
        description="Fetch citations for BBP articles."
    )
    parser.add_argument(
        "articles_csv", type=str, help="Path to the input CSV file."
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="output",
        help="Directory to save the output JSONL file.",
    )
    return parser


def main():
    """
    Execute the script.

    This function initializes the argument parser, retrieves the API key,
    processes the input CSV file to fetch citations, and logs the results.

    Args:
    ----
    None

    Returns:
    -------
    None
    """
    parser = get_parser()
    args = parser.parse_args()

    api_key = os.getenv("SERP_API_KEY")
    articles = args.articles_csv
    output_dir = args.output_dir

    checker = SerpApiCitationChecker(api_key, articles, output_dir)
    results = checker.process_csv()

    logging.info(f"Processed {len(results)} articles")
    logging.info(f"Number of SERP_API requests made: {checker.requests_made}")
    logging.info(
        "Exceptions saved to"
        f" {os.path.join(output_dir, 'serp_api_exceptions.json')}"
    )


if __name__ == "__main__":
    main()
