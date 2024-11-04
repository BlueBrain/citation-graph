"""Gather article metadata and article citations."""

import argparse
import json
import os
import pathlib
from datetime import date

import pandas as pd
from tqdm import tqdm

from citations.data_sources.europmc import (
    extract_authors,
    extract_bbp_article,
    fetch_article_element,
    get_citations,
)
from citations.schemas import Article, AuthorWroteArticle
from citations.utils import (
    generate_unique_id,
    is_valid_doi,
    load_europmc_xmls,
    to_date,
)


def set_article(
    abstract: str | None,
    articles: list[Article],
    author_wrote_article: list[AuthorWroteArticle],
    doi: str,
    europmc_ids: list[str],
    europmc_sources: list[str],
    isbns: str | None,
    publication_date: date | None,
    title: str,
    uids: list[str],
    url: str,
    is_published: bool,
    europmc_xml_map: dict,
    serp_article_map: dict,
) -> None:
    """
    Create and setup a new Article instance.

    Parameters
    ----------
    abstract : str
        The abstract of the article.
    articles : list
        The list of articles to add the new article to.
    author_wrote_article : list
        The list of author-article relationships to update.
    doi : str
        The DOI (Digital Object Identifier) of the article.
    europmc_ids : list
        The list of European PubMed Central (Europe PMC) IDs to update.
    europmc_sources : list
        The list of European PubMed Central (Europe PMC) sources to update.
    isbns : list
        The list of International Standard Book Numbers (ISBNs) of the article.
    publication_date : date | None
        The publication date of the article.
    title : str
        The title of the article.
    uids : list
        The list of unique IDs to update.
    url : str
        The URL of the article.
    is_published : bool
        Flag indicating if the article is published.

    """
    if is_published:
        article_element = fetch_article_element(doi, isbns, title)  # type: ignore
        if article_element is None:  # Try Serp API
            if title in serp_article_map:
                article_id = generate_unique_id(title)
                article = Article(
                    uid=article_id,  # type: ignore
                    title=title,
                    publication_date=publication_date,  # type: ignore
                    source="serp",
                    is_bbp=True,
                    is_published=True,
                    abstract=abstract,
                    doi=doi,
                    pmid=None,
                    europmc_id=None,
                    url=url,
                    isbns=isbns,
                )
                uids.append(article_id)
                articles.append(article)
                # TODO: implement author WROTE information to this block as well

        else:  # Use EuroPMC result
            article, europmc_source, europmc_id = extract_bbp_article(
                article_element,
                title,
                abstract,
                doi,
                publication_date,
                url,
                isbns,
            )
            europmc_xml_map[article.uid] = article_element
            europmc_ids.append(europmc_id)
            europmc_sources.append(europmc_source)
            uids.append(europmc_id)
            articles.append(article)
            orcid_ids = extract_authors(article_element)
            for orcidid in orcid_ids:
                author_wrote_article.append(
                    AuthorWroteArticle(
                        author_uid=orcidid, article_uid=europmc_id
                    )
                )
    else:  # If serp doesnt work, create Article class manualy from csv
        article_id = generate_unique_id(title)
        article = Article(
            uid=article_id,  # type: ignore
            title=title,
            publication_date=publication_date,  # type: ignore
            source="csv",
            is_bbp=True,
            is_published=False,
            abstract=abstract,
            doi=doi,
            pmid=None,
            europmc_id=None,
            url=url,
            isbns=isbns,
        )
        uids.append(article_id)
        articles.append(article)
        # TODO: implement author WROTE information to this block as well


def create_dict_from_jsons(directory):
    """
    Create a dictionary from jsons.

    Parameters
    ----------
    directory : str
        The directory path where the JSON files are located.

    Returns
    -------
    dict
        A dictionary where the keys are the file names (without the .json extension) and the values are the contents
        of the corresponding JSON files.

    """
    json_dict = {}

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            with open(os.path.join(directory, filename)) as f:
                json_content = json.load(f)
                # removing .json from the filename to use as the dict key
                key = filename[:-5]
                json_dict[key] = json_content

    return json_dict


def main(args):
    """Run the script."""
    bbp_publications = pd.read_csv(args.bbp_articles_path)
    bbp_publications["is_published"] = True
    bbp_wip_articles = pd.read_csv(args.bbp_articles_wip_path)
    bbp_wip_articles["is_published"] = False
    bbp_publications = pd.concat(
        [bbp_publications, bbp_wip_articles[bbp_publications.columns]],
        ignore_index=True,
    )
    bbp_wip_theses = pd.read_csv(args.bbp_theses_wip_path)
    bbp_wip_theses["is_published"] = False
    bbp_publications = pd.concat(
        [bbp_publications, bbp_wip_theses[bbp_publications.columns]],
        ignore_index=True,
    )
    bbp_publications = bbp_publications.drop_duplicates(subset=["Title"])
    bbp_publications = bbp_publications[~bbp_publications["Title"].isna()]

    serp_article_map = create_dict_from_jsons(args.serp_jsons_path)
    changed_map = {}
    for key in serp_article_map:
        filt = bbp_publications["Title"].apply(lambda title, k=key: k in title)
        titles = bbp_publications[filt]["Title"]
        if len(titles) > 0:
            changed_map[titles.iloc[0]] = serp_article_map[key]
    serp_article_map = changed_map

    uids = []
    articles = []
    article_cites_article = []
    europmc_sources = []
    europmc_ids = []
    author_wrote_article = []
    europmc_article_xmls_path = args.europmc_article_xmls_path
    europmc_xml_map = load_europmc_xmls(europmc_article_xmls_path)

    # Process all BBP articles before fetching the citations
    for _, bbp_publication_row in tqdm(
        bbp_publications.iterrows(), desc="Processing BBP articles"
    ):
        doi = bbp_publication_row["DOI"]
        if pd.isna(doi) or not is_valid_doi(doi):
            doi = None
        title = bbp_publication_row["Title"]
        abstract = bbp_publication_row["Abstract Note"]
        if pd.isna(abstract):
            abstract = ""
        url = bbp_publication_row["Url"]
        if pd.isna(url):
            url = None
        try:
            publication_date = to_date(bbp_publication_row["Date"])
            if pd.isna(publication_date):
                publication_date = None
        except AttributeError:
            publication_date = None
        except ValueError:
            publication_date = None
        isbns = bbp_publication_row["ISBN"]
        if pd.isna(isbns):
            isbns = None

        set_article(
            abstract,
            articles,
            author_wrote_article,
            doi,
            europmc_ids,
            europmc_sources,
            isbns,
            publication_date,
            title,
            uids,
            url,
            bbp_publication_row["is_published"],
            europmc_xml_map,
            serp_article_map,
        )

    # Process all articles citing BBP
    for europmc_id, europmc_source in tqdm(
        zip(europmc_ids, europmc_sources), desc="Processing citations"
    ):
        citations, citing_articles = get_citations(
            europmc_id, europmc_source, europmc_xml_map
        )
        for citing_article in citing_articles:
            if citing_article.uid not in uids:
                uids.append(citing_article.uid)
                articles.append(citing_article)
        article_cites_article.extend(citations)

    articles_dict = [article.model_dump() for article in articles]
    df = pd.DataFrame(articles_dict)

    # Define the columns to clean
    columns_to_clean = ["title", "abstract"]

    # Replace illegal characters in the specified columns
    for col in columns_to_clean:
        df[col] = (
            df[col].astype(str).str.replace('"', "")
        )  # Remove double quotes
        df[col] = (
            df[col].astype(str).str.replace("'", "")
        )  # Remove single quotes

    df.sort_values(by="uid", inplace=True)
    df.to_csv(os.path.join(args.output_dir, "articles.csv"), index=False)

    article_cites_article_dict = [
        citation.model_dump() for citation in article_cites_article
    ]
    df_cites = pd.DataFrame(article_cites_article_dict)
    df_cites.sort_values(
        by=["article_uid_source", "article_uid_target"], inplace=True
    )
    df_cites.to_csv(
        os.path.join(args.output_dir, "article_cites_article.csv"), index=False
    )
    author_wrote_article = [
        wrote.model_dump() for wrote in author_wrote_article
    ]
    author_wrote_article = pd.DataFrame(author_wrote_article)
    author_wrote_article.sort_values(
        by=["author_uid", "article_uid"], inplace=True
    )


def get_parser() -> argparse.ArgumentParser:
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "bbp_articles_path",
        type=pathlib.Path,
        help="Path the the csv file containing BBP publications",
    )
    parser.add_argument(
        "bbp_articles_wip_path",
        type=pathlib.Path,
        help=(
            "Path the the csv file containing work in progress BBP"
            " publications"
        ),
    )
    parser.add_argument(
        "bbp_theses_wip_path",
        type=pathlib.Path,
        help="Path the the csv file containing work in progress BBP theses",
    )
    parser.add_argument(
        "europmc_article_xmls_path",
        type=pathlib.Path,
        help="Path to the directory where we save and load europmc xmls",
    )
    parser.add_argument(
        "serp_jsons_path",
        type=pathlib.Path,
        help=(
            "Path to the directory where we save and load serp jsons for some"
            " articles."
        ),
    )
    parser.add_argument(
        "output_dir",
        type=pathlib.Path,
        help="Directory where we save all output files.",
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    main(args)
