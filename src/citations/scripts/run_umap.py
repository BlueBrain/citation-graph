"""Run UMAP on article embeddings and save the results to a JSON file."""

import argparse
import json
import logging

import numpy as np
import umap

from citations.schemas import DimensionReductionResult, UMAPParams

logging.basicConfig(level=logging.INFO)


def read_jsonl(file_path):
    """Read embeddings from a JSONL file."""
    embeddings = []
    uids = []
    with open(file_path, "r") as file:
        for line in file:
            data = json.loads(line)
            uids.append(data["article_uid"])
            embeddings.append(data["vector"])
    return uids, embeddings


def generate_umap_coordinates(embeddings, params: UMAPParams):
    """Generate UMAP coordinates for articles."""
    umap_model = umap.UMAP(
        n_neighbors=params.n_neighbors,
        n_components=params.n_components,
        metric=params.metric,
        min_dist=params.min_dist,
        random_state=params.random_state,
    )
    umap_coordinates = umap_model.fit_transform(np.array(embeddings))
    return umap_coordinates.tolist()


def create_dimension_reduction_result(uids, embeddings, reduced_coords, params):
    """Create DimensionReductionResult object."""
    return DimensionReductionResult(
        method="UMAP",
        params=params,
        article_uids=uids,
        embeddings=embeddings,
        reduced_dimensions=reduced_coords,
    )


def save_to_json(result: DimensionReductionResult, output_file):
    """Save DimensionReductionResult object to a JSON file."""
    with open(output_file, "w") as f:
        json.dump(result.model_dump(), f, indent=2)


def main(args):
    """Generate UMAP coordinates for articles and save them to a JSON file."""
    logging.info(f"Reading embeddings from {args.input_file}...")
    uids, embeddings = read_jsonl(args.input_file)

    umap_params = UMAPParams(n_components=args.n_components, random_state=args.random_state)

    logging.info("Generating UMAP coordinates...")
    reduced_coords = generate_umap_coordinates(embeddings, umap_params)

    logging.info("Creating DimensionReductionResult object...")
    result = create_dimension_reduction_result(uids, embeddings, reduced_coords, umap_params)

    logging.info(f"Saving results to {args.output_file}...")
    save_to_json(result, args.output_file)

    logging.info("Process completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Generate UMAP coordinates for articles and save them to a JSON" " file.")
    )
    parser.add_argument(
        "--input_file",
        type=str,
        required=True,
        help="Input JSONL file containing article embeddings",
    )
    parser.add_argument(
        "--n_components",
        type=int,
        default=2,
        help="Number of UMAP components (default: 2)",
    )
    parser.add_argument(
        "--random_state",
        type=int,
        default=42,
        help="Random state for UMAP (default: 42)",
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default="data/umap_results.json",
        help="Output JSON file name",
    )

    args = parser.parse_args()
    main(args)
