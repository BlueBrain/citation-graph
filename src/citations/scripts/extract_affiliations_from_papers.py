"""Gather author metadata from orcid including affiliations."""

import argparse
import logging
import os
import pathlib
from glob import glob
from io import StringIO

import jaro  # type: ignore
import numpy as np
import pandas as pd
from openai import OpenAI
from pypdf import PdfReader  # type: ignore
from tqdm import tqdm

QUESTION_TEMPLATE = """
You need to create a csv formatted output from a page of an article I will provide you.
Make sure to extract all article authors.

Example page:

Alessio Paolo Buccino∗1
, Tanguy Damart2
, Julian Bartram1
1Bio Engineering Laboratory, Department of Biosystems Science and Engineering,
ETH Zurich, Basel, Switzerland
2Blue Brain Project, École polytechnique fédérale de Lausanne (EPFL), Campus
Biotech, 1202 Geneva, Switzerland

Example output:

author name;department;institution;city;country
Alessio Paolo Buccino;Bio Engineering Laboratory, Department of Biosystems Science and Engineering;ETH Zurich;Basel;Switzerland
Tanguy Damart;Blue Brain Project;École polytechnique fédérale de Lausanne (EPFL);Geneva,Switzerland
Julian Bartram;Bio Engineering Laboratory, Department of Biosystems Science and Engineering;ETH Zurich;Basel;Switzerland
The actual article's page:

'{article}'
"""


CORRECTION_TEMPLATE = """
I am going to provide you a document. Each line contains some
metadata about an author, the department they are working for, the department's institution and the country the
institution is in.

All lines eithor miss some or all of the above information, or have excessive information.
I want you to reformat the whole text into a csv and fix the problems.
The columns should be:
author name;department;institution;city;country

For example from this line.
Mickael Zbili;Blue Brain Project;École polytechnique fédérale de Lausanne (EPFL);Campus Biotech;Geneva;Switzerland

the output should be
author name;department;institution;city;country
Mickael Zbili;Blue Brain Project;École polytechnique fédérale de Lausanne (EPFL);Geneva;Switzerland
because 'Campus Biotech' is not part of any of the required information, it should not be in the document.

If any information is missing, add 'unknown' in the right column. Use ";" as separator in the csv.

Only return lines where the information was in the following document.
The document:
{lines}
"""

logger = logging.getLogger(__name__)


def main(
    openai_api_key: str, raw_path: str, dedup_path: str, reparse_pdfs: bool
):
    """
    Collect all article pdfs and extract authors with affiliated departments.

    Parameters
    ----------
    openai_api_key : str
        The OpenAI API key used for authentication.

    raw_path : str
        The path to the raw CSV file where the parsed PDF data will be saved.

    dedup_path : str
        The path to the deduplicated CSV file where the final data will be saved.

    reparse_pdfs : bool
        Flag indicating whether to reparse the PDFs or not.

    Returns
    -------
    None

    """
    client = OpenAI(
        api_key=openai_api_key,
    )
    if os.path.exists(raw_path):
        if reparse_pdfs:
            df = parse_pdfs(args.pdfs_path, client)
            df.to_csv(raw_path, index=False)
    else:
        df = parse_pdfs(args.pdfs_path, client)
        df.to_csv(raw_path, index=False)
    df = pd.read_csv(raw_path)
    df = drop_missing(df)
    df.reset_index(inplace=True, drop=True)
    df = deduplicate_entities(df, "author name")
    df = deduplicate_entities(df, "department")
    df.drop_duplicates(subset=["author name", "department"], inplace=True)
    df.to_csv(dedup_path, index=False)


def drop_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop records where department is not specified.

    Parameters
    ----------
    df : pd.DataFrame
        The pandas DataFrame containing the data.

    Returns
    -------
    pd.DataFrame
        The modified DataFrame with missing values dropped and filtered departments.
    """
    df.dropna(subset=["author name", "department"], inplace=True)
    for columns in ["department", "author name"]:
        df = df[~df[columns].str.contains("unknown", case=False)]
        df = df[~df[columns].str.contains("not available", case=False)]
        df = df[~df[columns].str.contains("not provided", case=False)]
        df = df[~df[columns].str.contains("unspecified", case=False)]
        df = df[~df[columns].str.contains("not specified", case=False)]
        df = df[~df[columns].str.contains("-", case=False)]
    return df


def create_alias_dict(entities: pd.Series, alias_mtx: np.ndarray) -> dict:
    """Create alias dictionary from the alias matrix.

    Parameters
    ----------
    entities : pd.Series
        List of entities.
    alias_mtx : np.ndarray
        Dictionary representing alias matrix.

    Returns
    -------
    dict
        Alias dictionary.

    """
    alias_dict = {}

    for i, entity in enumerate(entities):
        if entity not in alias_dict:
            alias_dict[entity] = entity

        for j in range(len(entities)):
            if alias_mtx[i, j]:
                alias_entity = entities[j]
                if alias_entity not in alias_dict:
                    alias_dict[alias_entity] = entity

    return alias_dict


def deduplicate_entities(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Replace each alias of an entity instance with the original entity name.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing the entities to be deduplicated.
    column : str
        The column name in the DataFrame representing the entities.

    Returns
    -------
    pd.DataFrame
        The deduplicated DataFrame with the updated values in the specified column.

    """
    entities = df[column]
    jwm_sims = np.empty((len(entities), len(entities)))
    for i in tqdm(range(len(entities)), desc="Calculating jaro similarity"):
        for j in range(len(entities)):
            jwm_sims[i, j] = jaro.jaro_winkler_metric(entities[i], entities[j])
    jwm_mtx = jwm_sims >= 0.9

    if column == "department":
        dep_prefix = np.zeros((len(entities), len(entities)))
        for i in tqdm(range(len(entities)), desc="Calculating prefixes"):
            for j in range(len(entities)):
                dep_prefix[i, j] = (
                    entities[i] in entities[j] or entities[j] in entities[i]
                )
        alias_mtx = np.logical_or(jwm_mtx, dep_prefix)
    else:
        alias_mtx = jwm_mtx

    alias_dict = create_alias_dict(entities, alias_mtx)

    for _, row in df.iterrows():
        row[column] = alias_dict[row[column]]
    return df


def parse_pdfs(pdfs_path: str, client: OpenAI) -> pd.DataFrame:
    """
    Use OpenAI to extract authors and department from all article pdfs.

    Parameters
    ----------
    pdfs_path: str
        The path to the directory containing the PDF files.

    client: OpenAI
        The client object for interacting with the OpenAI API.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the extracted information from the PDF files.

    """
    pdfs = glob(os.path.join(pdfs_path, "**.pdf"))
    dfs = []
    bad_lines = []
    for pdf in tqdm(pdfs):
        reader = PdfReader(pdf)
        page = reader.pages[0]
        first_page = page.extract_text()

        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": QUESTION_TEMPLATE.format(article=first_page),
                }
            ],
            model="gpt-3.5-turbo",
        )
        csv_string = chat_completion.choices[0].message.content

        lines = []
        for line in csv_string.split("\n"):  # type: ignore
            if len(line.split(";")) == 5:
                lines.append(line)
            else:
                bad_lines.append(line)

        if len(lines) == 0:
            continue
        csv_string = "\n".join(lines)
        csv_file_like = StringIO(csv_string)
        df = pd.read_csv(csv_file_like, sep=";")
        dfs.append(df)
    authors = pd.concat(dfs, ignore_index=True)
    lines_per_iter = 5
    dfs = []
    for i in tqdm(range(0, len(bad_lines), lines_per_iter)):
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": CORRECTION_TEMPLATE.format(
                        lines="\n".join(bad_lines[i : i + lines_per_iter])
                    ),
                }
            ],
            model="gpt-3.5-turbo",
        )
        csv_string = chat_completion.choices[0].message.content
        csv_file_like = StringIO(csv_string)
        try:
            dfs.append(pd.read_csv(csv_file_like, sep=";"))
        except (pd.errors.ParserError, ValueError) as e:
            logging.warning(f"Error parsing CSV: {e}")
            continue
    df = pd.concat(dfs)
    authors = pd.concat([authors, df])
    authors.drop_duplicates(["author name", "department"], inplace=True)
    authors = drop_missing(authors)
    authors.sort_values(by=["author name", "department"], inplace=True)
    authors.reset_index(inplace=True, drop=True)
    return authors[
        ["author name", "department", "institution", "city", "country"]
    ]


def get_parser() -> argparse.ArgumentParser:
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "pdfs_path",
        type=pathlib.Path,
    )
    parser.add_argument("openai_api_key", default=16000)
    parser.add_argument("raw_path")
    parser.add_argument("dedup_path")
    parser.add_argument("--model_id", default="gpt-3.5-turbo")
    parser.add_argument("--max_tokens", type=int, default=16000)
    parser.add_argument(
        "--reparse_pdfs",
        action="store_true",
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    main(
        args.openai_api_key, args.raw_path, args.dedup_path, args.reparse_pdfs
    )
