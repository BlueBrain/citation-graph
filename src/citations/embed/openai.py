"""Embed articles using OpenAI API."""

import asyncio
import json
import logging
import os
from logging import getLogger
from typing import List, Literal

import aiohttp
import openai
import pandas as pd
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

logger = getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Load environment variables from .env file
load_dotenv()

# Load your OpenAI API key from environment variables
openai.api_key = os.getenv("OPENAI_API_KEY", "")


async def process_batches(session, missing_data, batch_size):
    """
    Process missing embeddings in batches.

    Parameters
    ----------
    session : aiohttp.ClientSession
        An aiohttp client session.
    missing_data : List[Dict[str, str]]
        A list of dictionaries containing article UIDs and text to embed.
    batch_size : int
        Number of articles to process in each batch.

    Returns
    -------
    List[Dict[str, Union[str, List[float]]]
        A list of dictionaries containing article UIDs and embeddings.
    """
    all_embeddings = []
    tasks = [fetch_embeddings(session, item["text"]) for item in missing_data]
    for i in tqdm(range(0, len(tasks), batch_size), desc="Processing batches"):
        batch_results = await asyncio.gather(*tasks[i : i + batch_size])
        for j, emb in enumerate(batch_results):
            if emb:
                all_embeddings.append(
                    {
                        "article_uid": missing_data[i + j]["article_uid"],
                        "vector": emb,
                    }
                )
    return all_embeddings


async def fetch_embeddings(
    session: aiohttp.ClientSession, text: str, retry_count: int = 3
) -> List[float] | None:
    """
    Fetch embeddings for a given text using OpenAI API.

    Parameters
    ----------
    session : aiohttp.ClientSession
        An aiohttp client session.
    text : str
        The text to embed.
    retry_count : int
        The number of times to retry the request on server errors.

    Returns
    -------
    List[float] | None
        A list of embeddings for the given text or None if the request fails.
    """
    for attempt in range(retry_count):
        try:
            response = await session.post(
                "https://api.openai.com/v1/embeddings",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {openai.api_key}",
                },
                json={
                    "input": text,
                    "model": "text-embedding-3-large",
                },
            )
            if response.status == 200:
                json_response = await response.json()
                return json_response["data"][0]["embedding"]
            elif response.status in {
                502,
                503,
                504,
            }:  # Retry on 502, 503, 504 errors
                logger.warning(f"Server error {response.status}, retrying...")
                await asyncio.sleep(2**attempt)  # Exponential backoff
            else:
                logger.error(
                    "Failed to get embeddings:"
                    f" {response.status} {await response.text()}"
                )
                return None
        except Exception as e:
            logger.error(f"Failed to get embeddings: {str(e)}")
            if attempt < retry_count - 1:
                await asyncio.sleep(2**attempt)  # Exponential backoff
            else:
                return None
    return None


def convert_article_uids_to_str(df):
    """Convert all non-string values in the 'article_uids' column to string.

    Parameters:
    ----------
    - df (pd.DataFrame): The input DataFrame containing the 'article_uids' column.

    Returns:
    -------
    - pd.DataFrame: The modified DataFrame with 'article_uids' converted to string.

    """
    # Iterate through the 'article_uids' column
    for index, row in df.iterrows():
        uid = row["article_uid"]

        # Check if the value is not a string
        if not isinstance(uid, str):
            # Convert the value to string
            df.at[index, "article_uid"] = str(uid)

            # Log a warning
            logger.warning(
                f"Converted non-string value '{uid}' to string in"
                " 'article_uid' column."
            )

    return df


def handle_duplicates(
    df: pd.DataFrame, option: Literal["discard", "use_first", "use_last"]
) -> pd.DataFrame:
    """
    Handle duplicates in the embeddings data frame.

    Parameters
    ----------
    df : pd.DataFrame
        Data frame containing embeddings.
    option : Literal['discard','use_first','use_last']
        The action to take for handling duplicates.

    Returns
    -------
    pd.DataFrame
        Data frame with duplicates resolved.
    """
    duplicates = df[df.duplicated(subset="article_uid", keep=False)]

    if not duplicates.empty:
        logger.info("Handling duplicates in the embeddings data frame.")

        unique_embeddings = {}
        for uid in duplicates["article_uid"].unique():
            duplicate_records = duplicates[duplicates["article_uid"] == uid]
            embeddings = duplicate_records["vector"].tolist()

            if option == "use_first":
                unique_embeddings[uid] = embeddings[0]
                logger.info(f"Using the first embedding for article_uid {uid}")
            elif option == "use_last":
                unique_embeddings[uid] = embeddings[-1]
                logger.info(f"Using the last embedding for article_uid {uid}")
            elif option == "discard":
                logger.info(
                    f"Discarding duplicate embeddings for article_uid {uid}"
                )

        # Update the data frame to keep only the resolved embeddings
        df = df.drop_duplicates(subset="article_uid", keep=False)
        for uid, embedding in unique_embeddings.items():
            df = df.append(  # type: ignore
                {"article_uid": uid, "vector": embedding}, ignore_index=True
            )
    return df


def save_embeddings_to_jsonl(
    df_combined: pd.DataFrame, embedded_file: str, model_name: str
):
    """
    Save the combined dataframe with embeddings to a JSON Lines file.

    Parameters
    ----------
    df_combined : pd.DataFrame
        The dataframe containing article UIDs and their corresponding embeddings.
    embedded_file : str
        The path to the output JSON Lines file.
    model_name : str
        The name of the model used to generate the embeddings.
    """
    with open(embedded_file, "w") as f:
        for _, row in df_combined.iterrows():
            try:
                embedding = row["vector"]
                # Create the full JSON object
                json_obj = {
                    "article_uid": row["article_uid"],
                    "model": model_name,
                    "vector": embedding,
                }
                # Write the JSON object to the file
                f.write(json.dumps(json_obj) + "\n")
            except Exception as e:
                logger.error(f"Failed to save embedding: {str(e)}")


def load_jsonl(file_path: str) -> pd.DataFrame:
    """Load a JSON Lines file into a DataFrame."""
    data = []
    with open(file_path, "r") as f:
        for line in f:
            data.append(json.loads(line))
    return pd.DataFrame(data)


async def main(
    data_dir: str,
    limit: int = -1,
    batch_size: int = 10,
    duplicate_strategy: Literal[
        "discard", "use_first", "use_last"
    ] = "discard",
) -> None:
    """
    Run code to embed articles using OpenAI API.

    Parameters
    ----------
    data_dir : str
        Path to the data directory.
    limit : int
        Number of articles to process.
        default: -1 (process all articles)
    batch_size : int
        Number of articles to process in each batch.
    duplicate_strategy : Literal['discard','use_first','use_last']
        The action to take for handling duplicates.
        discard: Discard duplicate embeddings.
        use_first: Use the first duplicate embedding.
        use_last: Use the last duplicate embedding.
        default: discard
    """
    # Path to data directory and files
    articles_file = os.path.join(data_dir, "articles.csv")
    embedded_file = os.path.join(data_dir, "articles_embedded.jsonl")

    # Load articles data
    df_articles = pd.read_csv(articles_file, dtype={"uid": str})

    # Limit the number of articles to process if the limit argument is provided
    if limit > 0:
        df_articles = df_articles.head(limit)

    # Load existing embeddings if they exist and omit the already existing article_uids
    embedded_ids = set()
    if os.path.exists(embedded_file):
        logger.info(f"Found existing embeddings file: {embedded_file}")
        df_embedded = load_jsonl(embedded_file)
        # Convert article_uids to string
        df_embedded = convert_article_uids_to_str(df_embedded)
        df_embedded = handle_duplicates(df_embedded, duplicate_strategy)  # type: ignore
        embedded_ids = set(df_embedded["article_uid"])
        # Convert ids to str
        embedded_ids = set(map(str, embedded_ids))
        logger.info(f"Loaded {len(embedded_ids)} existing embeddings.")
    else:
        logger.info("No existing embeddings found.")
        df_embedded = pd.DataFrame(columns=["article_uid", "vector"])

    # Determine which articles are missing embeddings
    missing_data = [
        {"article_uid": pid, "text": title + " " + abstract}
        for pid, title, abstract in zip(
            df_articles["uid"],
            df_articles["title"].fillna(""),
            df_articles["abstract"].fillna(""),
        )
        if pid not in embedded_ids
    ]
    logging.info(f"Found {len(missing_data)} articles missing embeddings.")

    if not missing_data:
        logger.info("No missing embeddings found. Exiting.")
        return

    # Process in batches
    all_embeddings = []

    # Setup aiohttp session for async API calls
    async with aiohttp.ClientSession() as session:
        all_embeddings = await process_batches(
            session, missing_data, batch_size
        )

    # Save new embeddings to a file
    if all_embeddings:
        new_embeddings_df = pd.DataFrame(all_embeddings)
        df_combined = pd.concat(
            [df_embedded, new_embeddings_df], ignore_index=True
        ).drop_duplicates(subset="article_uid", keep="last")
        save_embeddings_to_jsonl(
            df_combined, embedded_file, "text-embedding-3-large"
        )
        logger.info(f"New embeddings saved as {embedded_file}")
    else:
        logger.warning("No new embeddings to save.")
