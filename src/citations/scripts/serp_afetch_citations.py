"""Fetch SERP articles async."""

import argparse
import asyncio
import csv
import json
import logging
import os
import string
import time
from typing import Dict

import aiofiles
import aiohttp
from dotenv import load_dotenv
from tqdm import tqdm
from unidecode import unidecode

load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="data/serp/serp_api.log",
)

# Set up a separate logger for API requests
api_request_logger = logging.getLogger("api_requests")
api_request_logger.setLevel(logging.INFO)
# Create a file handler for the API request logger
handler = logging.FileHandler("data/serp/api_requests.log")  # Specify the log file name
# Create a formatter and add it to the handler
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
# Add the handler to the API request logger
api_request_logger.addHandler(handler)

# Prevent propagation to the root logger
api_request_logger.propagate = False


def normalize_title(title):
    """
    Normalize the title of an article.

    This function removes punctuation, converts the title to lowercase,
    and handles special characters.

    Args:
    ----
        title (str): The title of the article to be normalized.

    Returns:
    -------
        str: The normalized title with punctuation removed, converted to lowercase,
        and special characters handled.
    """
    punctuation = string.punctuation.replace("-", "")
    title = unidecode(title.lower())  # Handle special characters
    title = "".join(c for c in title if c not in punctuation)
    title = " ".join(title.split())  # Normalize whitespace
    return title


class AsyncSerpApiCitationChecker:
    """
    A class to asynchronously check citations using the SerpApi.

    This class provides methods to interact with the SerpApi to fetch citation
    data, manage API rate limits, and log API requests. It is designed to handle
    asynchronous operations using an aiohttp session.

    Attributes
    ----------
    api_key : str
        The API key for accessing the SerpApi.
    csv_file : str
        The path to the CSV file containing data to be processed.
    output_dir : str
        The directory where output files will be saved.
    base_url : str
        The base URL for the SerpApi.
    account_url : str
        The URL for checking the SerpApi account status.
    requests_made : int
        The number of API requests made in the current session.
    max_requests_per_hour : int
        The maximum number of requests allowed per hour.
    rate_limit_reached : bool
        A flag indicating if the rate limit has been reached.
    session : aiohttp.ClientSession
        The aiohttp session for making HTTP requests.

    Methods
    -------
    __init__(api_key, csv_file, output_dir)
        Initializes the AsyncSerpApiCitationChecker with the given API key, CSV file, and output directory.

    check_rate_limit()
        Asynchronously checks the rate limit status and updates the rate_limit_reached attribute.

    make_api_request(params)
        Asynchronously makes an API request with the given parameters and returns the response data.
    """

    def __init__(self, api_key: str, csv_file: str, output_dir: str):
        self.api_key = api_key
        self.csv_file = csv_file
        self.output_dir = output_dir
        self.base_url = "https://serpapi.com/search.json"
        self.account_url = "https://serpapi.com/account"
        self.requests_made = 0
        self.max_requests_per_hour = 1000
        self.rate_limit_reached = False
        os.makedirs(self.output_dir, exist_ok=True)
        self.session: aiohttp.ClientSession | None = None

    async def check_rate_limit(self):
        """Check the rate limit status and updates self.rate_limit_reached."""
        params = {"api_key": self.api_key}
        async with self.session.get(self.account_url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                self.rate_limit_reached = (
                    data["this_hour_searches"] >= self.max_requests_per_hour or data["total_searches_left"] <= 0
                )
                if self.rate_limit_reached:
                    logging.warning(f"Rate limit reached. Details: {data}")
            else:
                logging.error(f"Error checking rate limit: Status {response.status}")
                self.rate_limit_reached = True  # Assume rate limit reached on error

    async def make_api_request(self, params: Dict) -> Dict:
        """
        Make an asynchronous API request.

        Args:
        ----
            params (Dict): Parameters for the API request.

        Returns:
        -------
            Dict: The response data from the API request.

        Raises:
        ------
            Exception: If the rate limit is reached.
        """
        if self.rate_limit_reached:
            raise Exception("Rate limit reached")

        # Check rate limit before making the request
        await self.check_rate_limit()

        self.requests_made += 1

        if self.session is None:
            self.session = aiohttp.ClientSession()

        async with self.session.get(self.base_url, params=params) as response:
            response.raise_for_status()

            # Log the request and response
            api_request_logger.info(f"Request: {self.base_url}, Params: {params}")

            return await response.json()

    async def get_article_id(self, row: Dict) -> tuple[str | None, list]:
        """Fetch the article ID using various identifiers.

        This method attempts to retrieve the article ID by querying the
        Google Scholar engine with different identifiers such as title,
        DOI, PMID, URL, and ISBNs. It prioritizes these fields in the
        given order and returns the first matching article ID found.

        Args:
        ----
            row (Dict): A dictionary containing article information with
            potential keys: 'title', 'doi', 'pmid', 'url', 'isbns'.

        Returns:
        -------
            Optional[str]: The article ID if found, otherwise None.

        Raises:
        ------
            Exception: If the rate limit is reached during the API request.
        """
        if row is None:
            return None

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
                data = await self.make_api_request(params)
            except Exception as e:
                logging.error("Error fetching article ID for" f" {field_name} '{field_value}': {str(e)}")
                await self.save_exception("get_article_id", None, field_value, str(e))
                if self.rate_limit_reached:
                    raise
                continue

            for result in data.get("organic_results", []):
                # Normalize titles for comparison
                result_title = result["title"].lower().strip()
                input_title = title.lower().strip()

                # Remove trailing period if present
                result_title = result_title.rstrip(".")
                input_title = input_title.rstrip(".")

                input_title = normalize_title(input_title)
                result_title = normalize_title(result_title)

                if field_name == "title" and result_title == input_title:
                    # Fetch authors
                    authors = result.get("publication_info", {}).get("authors", [])
                    return result["result_id"], authors
                elif field_name in ["doi", "pmid", "url", "isbns"] and input_title == result_title:
                    # Fetch authors
                    authors = result.get("publication_info", {}).get("authors", [])
                    return result["result_id"], authors
                else:
                    logging.warning("Article ID not found for any field names for title:" f" {input_title}")
                    logging.debug(result)
                    is_same_title = input_title == result_title
                    if not is_same_title:
                        logging.warning(f"Title mismatch: {input_title} vs {result_title}")

        logging.warning(f"No article ID found for title: {title}")
        await self.save_exception(
            "get_article_id",
            None,
            title,
            "No article ID returned for any field.",
        )
        return None, []

    async def get_citations(self, article_id: str, title: str) -> Dict:
        """
        Fetch citations for a given article ID.

        This function retrieves citation data for a specified article
        using its unique article ID. It handles asynchronous operations
        to efficiently fetch data from the API.

        Args:
        ----
            article_id (str): The unique identifier for the article.
            title (str): The title of the article.

        Returns:
        -------
            Dict: A dictionary containing citation data for the article.
        """
        all_citations = []
        total_citations = 0
        start = 0

        # Load existing citations for this article, if any
        citations_file = os.path.join(self.output_dir, f"citations_{article_id}.json")
        if os.path.exists(citations_file):
            async with aiofiles.open(citations_file, "r") as f:
                data = json.loads(await f.read())
                total_citations = data.get("total_citations", 0)
                all_citations = data.get("citations", [])
                logging.info(
                    f"Loaded {len(all_citations)} existing citations for"
                    f" article '{title}' (ID: {article_id}) from"
                    f" {citations_file}"
                )

        try:
            # First request to get total_citations (only if we don't have it already)
            if total_citations == 0:
                params = {
                    "engine": "google_scholar",
                    "cites": article_id,
                    "start": start,
                    "num": 20,
                    "api_key": self.api_key,
                    "hl": "en",
                }
                data = await self.make_api_request(params)
                total_citations = int(data["search_information"]["total_results"])

            # If there are many citations, log this information
            if total_citations > 100:
                logging.info(
                    f"Article '{title}' (ID: {article_id}) has" f" {total_citations} citations. This may take a while."
                )

            with tqdm(
                total=min(total_citations, 1000),
                desc=f"Fetching citations for '{title}'",
                initial=start,
                leave=False,
            ) as pbar:
                while start < min(
                    total_citations, 1000
                ):  # Limit to 1000 citations due to Google Limit or total_citations
                    params = {
                        "engine": "google_scholar",
                        "cites": article_id,
                        "start": start,
                        "num": 20,
                        "api_key": self.api_key,
                        "hl": "en",
                    }
                    data = await self.make_api_request(params)

                    if total_citations == 0:
                        total_citations = int(data["search_information"]["total_results"])

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
                            await self.save_exception(
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
                        missing_fields = [field for field, value in citation_info.items() if value is None]
                        if missing_fields:
                            await self.save_exception(
                                "get_citations",
                                citation.get("result_id"),
                                article_id,
                                f"Missing fields: {', '.join(missing_fields)}",
                            )

                    # Update the progress bar
                    pbar.update(len(citations))

                    start += 20

                    # Log progress for highly cited articles
                    if total_citations > 100 and len(all_citations) % 100 == 0:
                        logging.info(
                            "Fetched"
                            f" {len(all_citations)}/{total_citations} citations"
                            f" for article '{title}' (ID: {article_id})"
                        )

        except Exception as e:
            logging.error(f"Error fetching citations for article ID '{article_id}'" f" (Title: '{title}'): {str(e)}")
            await self.save_exception("get_citations", None, article_id, str(e))
            if self.rate_limit_reached:
                # Save citations to file only if rate limit is reached
                async with aiofiles.open(citations_file, "w") as f:
                    await f.write(
                        json.dumps(
                            {
                                "total_citations": total_citations,
                                "citations": all_citations,
                            }
                        )
                    )

                logging.warning(
                    "Rate limit reached while fetching citations for"
                    f" '{title}'. Progress saved. You can resume later."
                )
            else:
                raise  # Re-raise other exceptions

        # If we hit the 1000 citation limit, log a warning
        if total_citations > 1000:
            logging.warning(
                f"Article '{title}' (ID: {article_id}) has more than 1000"
                " citations. Only the first 1000 were fetched due to Google"
                " Limit."
            )

        return {"total_citations": total_citations, "citations": all_citations}

    async def save_exception(
        self,
        function: str,
        citing_article_identifier: str | None,
        cited_article_identifier: str,
        reason: str,
    ):
        """
        Save exception details to a JSON file.

        This method logs exceptions that occur during the citation fetching
        process, including details about the function where the exception
        occurred, the article identifiers involved, and the reason for the
        exception.

        Args:
        ----
            function (str): The name of the function where the exception occurred.
            citing_article_identifier (Optional[str]): The identifier of the citing
                article, if applicable.
            cited_article_identifier (str): The identifier of the cited article.
            reason (str): A description of the reason for the exception.

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
        async with aiofiles.open(os.path.join(self.output_dir, "serp_api_exceptions.json"), "a") as f:
            await f.write(json.dumps(exception) + "\n")

    async def process_row(self, row: Dict, fetch_citations: bool = False) -> Dict | None:
        """
        Process a single row of data.

        This method processes a row from the CSV file, fetching the article ID
        and authors if the article is a BBP thesis and is published. Optionally,
        it can also fetch citations for the article.

        Args:
        ----
            row (Dict): A dictionary representing a row of data from the CSV file.
            fetch_citations (bool): A flag indicating whether to fetch citations
                for the article. Defaults to False.

        Returns:
        -------
            Dict | None: A dictionary containing the processed article data,
            including title, article ID, authors, and optionally citations. Returns
            None if the article ID is not found or the article does not meet the
            criteria for processing.
        """
        if row.get("is_bbp") == "True" and row.get("is_published") == "True":
            article_id, authors = await self.get_article_id(row)
            if article_id:
                result = {
                    "title": row["title"],
                    "article_id": article_id,
                    "authors": authors,
                }

                if fetch_citations:
                    citations = await self.get_citations(article_id, row["title"])
                    result["total_citations"] = citations["total_citations"]
                    result["citations"] = citations["citations"]

                # If the source is 'serp_csv' and there were no authors before, update the row
                if row.get("source") == "serp_csv" and not row.get("authors") and authors:
                    row["authors"] = authors
                return result
            else:
                logging.warning(f"No article ID found for title: {row['title']}")
                await self.save_exception(
                    "get_article_id",
                    None,
                    row["title"],
                    "No article ID returned.",
                )

        return None

    async def process_csv(self):
        """
        Process the CSV file to fetch and store citation data.

        This method reads the CSV file specified by `self.csv_file`, filters
        the articles based on certain criteria, and processes each article
        to fetch citation data. The results are stored in a JSONL file in
        the specified output directory.

        The method ensures that articles already processed are skipped to
        avoid redundant operations.

        Raises:
        ------
            FileNotFoundError: If the specified CSV file does not exist.
            json.JSONDecodeError: If there is an error decoding JSON data
            from the existing results file.
        """
        num_results = 0
        output_file = os.path.join(self.output_dir, "serp_citation_results.jsonl")
        logging.info(f"\n\nStarting Processing articles from {self.csv_file}")

        # Load existing data
        existing_data = set()
        if os.path.exists(output_file):
            async with aiofiles.open(output_file, "r") as f:
                async for line in f:
                    try:
                        data = json.loads(line)
                        existing_data.add(normalize_title(data["title"]))
                    except json.JSONDecodeError:
                        logging.error(f"Error loading JSON from line: {line}")
                        continue
        logging.info(f"Loaded {len(existing_data)} existing articles from {output_file}")

        # Count and filter articles to be processed in a single pass
        articles_to_process = []  # Store rows to process, not just the count
        async with aiofiles.open(self.csv_file, "r") as file:
            reader = csv.DictReader((await file.read()).splitlines())
            for row in reader:
                incoming_title = normalize_title(row["title"])
                if (
                    incoming_title not in existing_data
                    and row.get("is_bbp") == "True"
                    and row.get("is_published") == "True"
                ):
                    articles_to_process.append(row)
                else:
                    logging.info(
                        f"Skipping article '{row['title']}' as it is already" " processed or doesn't meet criteria."
                    )

        # Now process the filtered articles
        async with aiohttp.ClientSession() as self.session:
            with tqdm(total=len(articles_to_process), desc="Processing articles") as pbar:
                for row in articles_to_process:
                    try:
                        row_result = await self.process_row(row, fetch_citations=True)
                        if row_result:
                            num_results += 1
                            async with aiofiles.open(output_file, "a") as f:
                                await f.write(json.dumps(row_result) + "\n")
                    except Exception as e:
                        if self.rate_limit_reached:
                            logging.warning("Rate limit reached. Stopping processing.")
                            break
                        else:
                            logging.error("Error processing row for title" f" '{row['title']}': {str(e)}")
                    # Update progress bar only if article was processed
                    pbar.update(1)

                    if self.rate_limit_reached:
                        logging.warning("Rate limit reached. Stopping processing.")
                        break

        logging.info(f"Results saved to {output_file}")
        return num_results


def get_parser():
    """
    Create and return the argument parser for the script.

    This function sets up the command-line interface for the script,
    defining the necessary arguments and options that can be passed
    to the script when it is executed.

    Returns:
    -------
    argparse.ArgumentParser
        The argument parser with the defined arguments and options.

    Arguments:
    ---------
    articles_csv : str
        The path to the input CSV file containing articles.
    --output_dir : str, optional
        The directory where the output JSONL file will be saved.
        Defaults to 'output'.
    """
    parser = argparse.ArgumentParser(description="Fetch citations for BBP articles.")
    parser.add_argument("articles_csv", type=str, help="Path to the input CSV file.")
    parser.add_argument(
        "--output_dir",
        type=str,
        default="output",
        help="Directory to save the output JSONL file.",
    )
    return parser


async def main():
    """
    Execute the script.

    This function initializes the argument parser, retrieves the necessary
    environment variables, and processes the input CSV file to fetch
    citations using the SerpApi.

    Args:
    ----
    None

    Returns:
    -------
    None

    Raises:
    ------
    Exception: If there is an error during the execution of the script.
    """
    parser = get_parser()
    args = parser.parse_args()

    api_key = os.getenv("SERP_API_KEY")
    articles = args.articles_csv
    output_dir = args.output_dir

    checker = AsyncSerpApiCitationChecker(api_key, articles, output_dir)
    num_results = await checker.process_csv()

    logging.info(f"Processed {num_results} articles for citations.")
    logging.info(f"Number of SERP_API requests made: {checker.requests_made}")
    logging.info("Exceptions saved to" f" {os.path.join(output_dir, 'serp_api_exceptions.json')}")


if __name__ == "__main__":
    asyncio.run(main())
