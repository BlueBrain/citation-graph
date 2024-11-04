"""Gather author, institution metadata based on predownloaded profiles."""

import argparse
import logging
import os
import pathlib
import re
from glob import glob
from xml.etree import ElementTree as ET

import pandas as pd
from tqdm import tqdm

from citations.data_sources.orcid import (
    NAMESPACES,
    get_author_affiliations,
    get_author_name,
)
from citations.schemas import Author, AuthorWroteArticle

logger = logging.getLogger(__name__)


def load_europmc_article_author_mapping(europmc_article_xmls_path: str, article_author_mapping: dict):
    """
    Load EuroPMC xml files.

    Parameters
    ----------
    europmc_article_xmls_path : str
        The path to the directory containing European PubMed Central (Europe PMC) XML files.

    Returns
    -------
    xml_map : dict
    """
    for filename in os.listdir(europmc_article_xmls_path):
        if filename.endswith(".xml"):
            file_path = os.path.join(europmc_article_xmls_path, filename)
            tree = ET.parse(file_path)
            root = tree.getroot()
            file_key = os.path.splitext(filename)[0]
            orcidids = [
                elem.text
                for elem in root.findall(
                    "./resultList/result/authorIdList/authorId[@type='ORCID']",
                    namespaces=NAMESPACES,
                )
            ]
            if file_key in article_author_mapping:
                article_author_mapping[file_key] = article_author_mapping[file_key] + orcidids
            else:
                article_author_mapping[file_key] = orcidids
    return article_author_mapping


def main(
    articles_path,
    articles_europmc_xml_path,
    article_author_xml_path_doi,
    article_author_xml_path_author_name,
    authors_xml_path,
    output_dir,
):
    """Run the script."""
    article_uids = list(pd.read_csv(articles_path)["uid"])
    article_author_xml_paths = glob(os.path.join(article_author_xml_path_doi, "*.xml"))
    article_author_xml_paths.extend(
        [
            path
            for path in glob(os.path.join(article_author_xml_path_author_name, "*.xml"))
            if path not in article_author_xml_paths
        ]
    )
    article_author_mapping = {}
    for article_xml_path in tqdm(article_author_xml_paths, desc="Mapping articles to author orcidids"):
        basename = os.path.basename(article_xml_path)
        article_uid = basename[:-4]
        if article_uid not in article_uids:
            continue
        tree = ET.parse(article_xml_path)
        element = tree.getroot()
        orcidids = [orcid.text for orcid in element.findall(".//common:path", namespaces=NAMESPACES)]
        article_author_mapping[article_uid] = orcidids

    load_europmc_article_author_mapping(articles_europmc_xml_path, article_author_mapping)

    author_xml_paths = glob(os.path.join(authors_xml_path, "*.xml"))
    author_record_map = {}
    for xml_path in tqdm(author_xml_paths, desc="Collecting author orcid records"):
        basename = os.path.basename(xml_path)
        orcidid = basename[:-4]
        if orcidid not in author_record_map:
            tree = ET.parse(xml_path)
            element = tree.getroot()
            author_record_map[orcidid] = element

    authors = []
    for author_id in tqdm(list(author_record_map.keys()), desc="Building authors.csv"):
        record = author_record_map[author_id]
        name = get_author_name(record)
        if bool(re.match(r"\b\d{4}-\d{4}-\d{4}-\d{3}[0-9X]\b", author_id)):
            author = Author(uid=author_id, orcid_id=author_id, name=name)
        else:
            author = Author(uid=author_id, google_scholar_id=author_id, name=name)
        authors.append(author)
    authors = [author.model_dump() for author in authors]
    authors = pd.DataFrame(authors)
    authors.sort_values(by="uid", inplace=True)
    authors.drop_duplicates(subset=["uid"], inplace=True)
    authors.to_csv(os.path.join(output_dir, "authors.csv"), index=False)

    author_wrote_article = []
    for article_uid in tqdm(article_author_mapping, desc="Building author_wrote_article.csv"):
        for orcidid in article_author_mapping[article_uid]:
            author_wrote_article.append(AuthorWroteArticle(author_uid=orcidid, article_uid=article_uid))
    author_wrote_article = [wrote.model_dump() for wrote in author_wrote_article]
    author_wrote_article = pd.DataFrame(author_wrote_article)
    author_wrote_article.sort_values(by=["author_uid", "article_uid"], inplace=True)
    author_wrote_article.drop_duplicates(subset=["author_uid", "article_uid"], inplace=True)
    author_wrote_article.to_csv(os.path.join(output_dir, "author_wrote_article.csv"), index=False)

    institutions = []
    all_institution_ids = []
    author_aff_institution = []
    for orcidid in tqdm(author_record_map, desc="Building affiliations"):
        record = author_record_map[orcidid]
        author_institutions, affiliations = get_author_affiliations(orcidid, record)
        for institution in author_institutions:
            if institution.uid not in all_institution_ids:
                all_institution_ids.append(institution.uid)
                institutions.append(institution)
        author_aff_institution.extend(affiliations)
    author_aff_institution = [affiliation.model_dump() for affiliation in author_aff_institution]
    author_aff_institution = pd.DataFrame(author_aff_institution)
    author_aff_institution.sort_values(by=["author_uid", "institution_uid"], inplace=True)
    author_aff_institution.drop_duplicates(subset=["author_uid", "institution_uid"], inplace=True)
    author_aff_institution.to_csv(
        os.path.join(args.output_dir, "author_affiliated_with_institution.csv"),
        index=False,
    )
    institutions = [institution.model_dump() for institution in institutions]
    institutions = pd.DataFrame(institutions)
    institutions.sort_values(by="uid", inplace=True)
    institutions.drop_duplicates(subset=["uid"], inplace=True)
    institutions.to_csv(os.path.join(args.output_dir, "institutions.csv"), index=False)


def get_parser() -> argparse.ArgumentParser:
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--articles_path",
        default="data/articles.csv",
        type=pathlib.Path,
    )
    parser.add_argument(  # from europmc for articles
        "--articles_europmc_xml_path",
        default="data/publication_data/articles",
        type=pathlib.Path,
    )
    parser.add_argument(  # list of authors for bbp + bbp citing articles created with fetch_orcid_article_author_records.py
        "--article_author_xml_path_doi",
        default="data/publication_data/article_authors/doi",
        type=pathlib.Path,
    )
    parser.add_argument(
        "--article_author_xml_path_author_name",
        default="data/publication_data/article_authors/author_name",
        type=pathlib.Path,
    )
    parser.add_argument(
        "--author_profiles_xml_path",
        default="data/publication_data/author_profiles/orcid",
        type=pathlib.Path,
    )
    parser.add_argument(
        "--output_dir",
        type=pathlib.Path,
        default="data",
        help="Directory where we save all output files.",
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    main(
        args.articles_path,
        args.articles_europmc_xml_path,
        args.article_author_xml_path_doi,
        args.article_author_xml_path_author_name,
        args.author_profiles_xml_path,
        args.output_dir,
    )
