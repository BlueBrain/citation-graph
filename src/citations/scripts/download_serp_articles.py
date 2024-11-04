"""Download article jsons from Serp API."""

import argparse
import json
import os.path
from pathlib import Path

import pandas as pd
import serpapi
from tqdm import tqdm


def get_parser() -> argparse.ArgumentParser:
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "bbp_publications_path",
        type=Path,
        help="Path to the publications file.",
    )
    parser.add_argument(
        "serp_api_key",
    )
    parser.add_argument(
        "serp_output_path",
        type=Path,
        help="Output directory to where serp jsons are saved.",
    )
    parser.add_argument(
        "--articles_path",
        type=Path,
        help="Optional path to already processed articles.",
    )
    return parser


def main(args):
    """Run script."""
    bbp_publications = pd.read_csv(args.bbp_publications_path)
    if args.articles_path:
        processed_article_titles = pd.read_csv(args.articles_path)["title"]
    else:
        processed_article_titles = []

    serp_output_path = args.serp_output_path
    bbp_publications = bbp_publications[
        ~bbp_publications["Title"].isin(processed_article_titles)
    ]
    for _, bbp_publication_row in tqdm(
        bbp_publications.iterrows(), desc="Processing BBP articles"
    ):
        title = bbp_publication_row["Title"]
        search_result = serpapi.search(
            q=title, engine="google_scholar", api_key=args.serp_api_key
        )
        file_path = os.path.join(
            serp_output_path, f"{title.replace(os.path.sep, '')}.json"
        )
        if len(file_path) > 255:
            file_path = file_path[:250] + ".json"

        if os.path.exists(file_path):
            continue

        with open(file_path, "w") as f:
            f.write(json.dumps(dict(search_result)))


if __name__ == "__main__":
    """Run the script."""
    parser = get_parser()
    args = parser.parse_args()

    main(args)
