"""Gather author profile information using Serp API."""

import argparse
import json
import os
import pathlib

import pandas as pd
import serpapi
from tqdm import tqdm


def main(args):
    """Search author profiles with Serp and save results as jsons."""
    bbp_publications = pd.read_csv(args.bbp_articles_path)
    if args.bbp_articles_wip_path:
        bbp_wip_articles = pd.read_csv(args.bbp_articles_wip_path)
        bbp_publications = pd.concat([bbp_publications, bbp_wip_articles])
    if args.bbp_theses_wip_path:
        bbp_wip_theses = pd.read_csv(args.bbp_theses_wip_path)
        bbp_publications = pd.concat([bbp_publications, bbp_wip_theses])

    unique_authors = set()
    for _, row in tqdm(bbp_publications.iterrows()):
        if not isinstance(row["Author"], str):
            continue
        authors = row["Author"].split(";")
        for author_name in authors:
            author_name = author_name.strip()
            if author_name in unique_authors:
                continue
            else:
                unique_authors.add(author_name)

            name_parts = author_name.strip().split(",")
            if len(name_parts) == 2:
                family_name, given_names = name_parts
                family_name = family_name.strip()
                given_names = given_names.strip()
                full_name = f"{given_names} {family_name}"
            else:
                full_name = author_name.strip()
            path = os.path.join(args.output_directory, f"{full_name}.json")
            if os.path.exists(path):
                continue
            page = serpapi.search(
                mauthors=full_name,
                engine="google_scholar_profiles",
                api_key=args.serp_api_key,
            )
            with open(path, "w") as f:
                f.write(json.dumps(dict(page)))


def get_parser() -> argparse.ArgumentParser:
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "bbp_articles_path",
        type=pathlib.Path,
        help="Path the the csv file containing BBP publications and theses.",
    )
    parser.add_argument(
        "output_directory",
        type=pathlib.Path,
        help=(
            "Path the the output directory where we will store the author"
            " profile jsons."
        ),
    )
    parser.add_argument(
        "serp_api_key",
        type=pathlib.Path,
        help="Serp API key.",
    )
    parser.add_argument(
        "--bbp_articles_wip_path",
        type=pathlib.Path,
        help=(
            "Path the the csv file containing work in progress BBP"
            " publications"
        ),
    )
    parser.add_argument(
        "--bbp_theses_wip_path",
        type=pathlib.Path,
        help="Path the the csv file containing work in progress BBP theses",
    )
    parser.add_argument("--serp_profiles_path", type=pathlib.Path)

    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    main(args)
