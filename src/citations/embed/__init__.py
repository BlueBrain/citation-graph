"""Init function for the embed package."""

import json
from typing import Dict, List


def load_embeddings(file_path: str) -> Dict[str, List[float]]:
    """
    Load and validate embeddings from a JSON Lines file and return a dictionary.

    Parameters
    ----------
    file_path : str
        Path to the JSON Lines file.

    Returns
    -------
    Dict[str, List[float]]
        Dictionary with article_uids as keys and embeddings as values.
    """
    embeddings_dict = {}

    with open(file_path, "r") as file:
        for line in file:
            data = json.loads(line)
            embeddings_dict[data["article_uid"]] = data["vector"]

    return embeddings_dict
