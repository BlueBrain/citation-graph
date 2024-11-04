"""Main script to embed articles using OpenAI's API service."""

import argparse
import asyncio

from citations.embed.openai import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument(
        "--data_dir",
        type=str,
        default="data/",
        help="Path to the data directory",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Number of articles to process",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=10,
        help="Number of articles to process in each batch",
    )

    parser.add_argument(
        "--duplicate_strategy",
        type=str,
        default="discard",
        choices=["discard", "use_first", "use_last"],
        help="The action to take for handling duplicates",
    )

    args = parser.parse_args()
    asyncio.run(main(args.data_dir, args.limit, args.batch_size))
