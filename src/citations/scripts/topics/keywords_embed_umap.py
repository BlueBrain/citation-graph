"""Embed keywords using OpenAI API and reduce dimensions with UMAP."""

import argparse
import asyncio
import json
import logging
import os
from typing import Any, Dict, List

import umap
from dotenv import load_dotenv
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_random_exponential
from tqdm.asyncio import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Initialize AsyncOpenAI client
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))


@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
async def fetch_embeddings(text: str, model: str) -> List[float] | None:
    """Fetch embeddings for a given text using OpenAI API with exponential backoff."""
    try:
        response = await client.embeddings.create(input=text, model=model)
        return response.data[0].embedding
    except Exception as e:
        logger.error(f"Failed to get embeddings: {str(e)}")
        raise


async def embed_keywords(
    keywords: List[str], model: str, batch_size: int = 10
) -> List[Dict[str, Any]]:
    """Embed keywords using OpenAI API."""
    all_embeddings = []
    tasks = [fetch_embeddings(keyword, model) for keyword in keywords]
    for i in tqdm(
        range(0, len(tasks), batch_size), desc="Processing keyword batches"
    ):
        batch_results = await asyncio.gather(*tasks[i : i + batch_size])
        for j, emb in enumerate(batch_results):
            if emb:
                all_embeddings.append(
                    {
                        "keyword": keywords[i + j],
                        "model": model,
                        "vector": emb,
                    }
                )
    return all_embeddings


def perform_umap(
    embeddings: List[List[float]],
    n_neighbors: int = 15,
    n_components: int = 2,
    metric: str = "euclidean",
    min_dist: float = 0.1,
    random_state: int = 42,
):
    """Perform UMAP dimensionality reduction on the embeddings."""
    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        n_components=n_components,
        metric=metric,
        min_dist=min_dist,
        random_state=random_state,
    )
    reduced_embeddings = reducer.fit_transform(embeddings)
    return reduced_embeddings, reducer.get_params()


def load_existing_embeddings(file_path: str) -> Dict[str, Dict[str, Any]]:
    """Load existing embeddings from a JSONL file."""
    existing_embeddings = {}
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            for line in f:
                data = json.loads(line)
                existing_embeddings[data["keyword"]] = data
    return existing_embeddings


async def main(
    data_dir: str,
    model: str = "text-embedding-3-large",
    batch_size: int = 10,
    n_keywords: int = 10,
):
    """Run code to embed keywords using OpenAI API and perform UMAP."""
    # Path to data directory and files
    keywords_file = os.path.join(data_dir, "updated_article_keywords.json")
    keywords_embedded_file = os.path.join(
        data_dir, "keywords_embedded_test.jsonl"
    )
    keywords_umap_file = os.path.join(data_dir, "keywords_umap_test.json")

    # Load keywords
    with open(keywords_file, "r") as f:
        keywords_dict = json.load(f)

    # Extract unique keywords
    unique_keywords = list(
        {kw for kws in keywords_dict.values() for kw in kws}
    )
    logger.info(f"Found {len(unique_keywords)} unique keywords.")

    # Take only the first n_keywords
    if n_keywords > 0:
        unique_keywords = unique_keywords[:n_keywords]
        logger.info(f"Using {len(unique_keywords)} keywords for embedding.")

    # Load existing embeddings
    existing_embeddings = load_existing_embeddings(keywords_embedded_file)
    # check if existing embeddings is not empty
    if existing_embeddings != {}:
        if any(
            item["model"] != model for item in existing_embeddings.values()
        ):
            logger.warning("Existing embeddings are for a different model.")
            existing_embeddings = {}
        else:
            logger.info(
                f"Loaded {len(existing_embeddings)} existing embeddings."
            )
    else:
        logger.info("No existing embeddings found.")

    # Identify new keywords that need embedding
    new_keywords = [
        kw
        for kw in unique_keywords
        if kw not in existing_embeddings
        or existing_embeddings[kw]["model"] != model
    ]
    logger.info(f"Found {len(new_keywords)} new keywords to embed.")

    if new_keywords:
        # Embed new keywords
        new_embeddings = await embed_keywords(new_keywords, model, batch_size)

        # Combine new and existing embeddings
        all_embeddings = list(existing_embeddings.values()) + new_embeddings
        # Save all keyword embeddings
        with open(keywords_embedded_file, "w") as f:
            for item in all_embeddings:
                json.dump(item, f)
                f.write("\n")

        logger.info(
            f"All keyword embeddings saved as {keywords_embedded_file}"
        )
    else:
        all_embeddings = list(existing_embeddings.values())
        logger.info("No new keywords to embed.")

    # Perform UMAP
    embeddings = [item["vector"] for item in all_embeddings]
    reduced_embeddings, umap_params = perform_umap(embeddings)

    # Save UMAP results
    umap_results = {
        "method": "UMAP",
        "params": umap_params,
        "keywords": [item["keyword"] for item in all_embeddings],
        "reduced_dimensions": reduced_embeddings.tolist(),
    }
    with open(keywords_umap_file, "w") as f:
        json.dump(umap_results, f, indent=2)

    logger.info(f"UMAP results saved as {keywords_umap_file}")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Embed keywords using OpenAI API and perform UMAP."
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Path to data directory with keywords.",
    )
    parser.add_argument(
        "--model",
        default="text-embedding-3-large",
        help="OpenAI model to use.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for embedding requests.",
    )
    parser.add_argument(
        "--n-keywords",
        type=int,
        default=-1,
        help="Number of keywords to embed.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    asyncio.run(
        main(args.data_dir, args.model, args.batch_size, args.n_keywords)
    )
