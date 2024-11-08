# Description

Below is the pipeline to generate citation graph and load it into neo4j. Please read the notes at the end first if you want to use it with AuraDB.

[1. Gathering articles with citation data from EuroPMC](#gathering-articles-with-citation-data-from-europmc)

[2. Gathering author with affiliation data from Orcid](#gathering-author-with-affiliation-data-from-orcid)

[3. Fetch citations from Google Scholar]()

[4. Fetch citations from articles not available online]()

[5. Embed articles with their title + abtract values]()

[6. Cluster articles from their embeddings]()

[7. Dimension Reduction of Embeddings]()

[8. Creating extended article]()

[9. Loading extented articles to neo4j]()

[10. Generating Keywords]()

## Gathering Authors and Articles

## 1. Gathering articles with citation data from EuroPMC

To run the script `gather_articles.py` located in the `scripts` directory, follow these steps:

1. **Ensure you have the required CSV file** containing BBP publications.
2. **Run the script** using the following command:

```bash
python scripts/gather_articles.py /path/to/bbp_articles.csv
```

## 2. Gathering author with affiliation data from Orcid

To run the script `gather_authors.py` located in the `scripts` directory, follow these steps:

1. **Ensure you have the required CSV file** containing articles gathered by the script "gather_articles.py".
2. **Run the script** using the following command:

```bash
python scripts/gather_authors.py /path/to/articles.csv
```
## 3. Fetch citations from Google Scholar

Run 

```bash
python src/citations/scripts/combine_serp.py --input {path/to/serp_citation_results.jsonl} --output {path_to_output} --data_dir data/
```

which will update articles.csv authors.csv author_wrote_article.csv article_cites_article.csv with SERP api data.
The institutions wont be updated as this field is not available for google scholar directly.

## 4. Fetch citations from articles not available online

if both cases dont work because article is not published etc, we create Article entity manually from whatever data is available.

First you will need to fetch the publication data from the S3 bucket.

You can find it [here](https://us-east-1.console.aws.amazon.com/s3/buckets/citation-project-dvc?region=us-east-1&bucketType=general&tab=objects)

Download the zip file and decompress it somewhere. Then you can
run the script.

Example:

```bash
python src/citations/scripts/gather_authors.py 
  data/articles.csv 
  data/publication_data/articles/orcid 
  data/publication_data/articles/author_name 
  data/publication_data/author_profiles/orcid 
  data
```

## 5. Embed articles with their title + abtract values

```bash
python src/citations/scripts/embed_openai.py --data_dir /path/to/citation-graph/data 
```

## 6. Cluster articles from their embeddings

Optimization of hyperparameters is available with --optimize where in the script the range of hyperparameters will be 
searched given their silhouette score.
    
```bash
# For Agglomerative Clustering
python src/citations/scripts/optimize_clustering.py data/articles_embedded.jsonl data/clustering/clusters_agglomerative_kwargs.json agglomerative **kwargs

# For KMeans Clustering
python src/citations/scripts/optimize_clustering.py data/articles_embedded.jsonl data/clustering/clusters_kmeans_k.json kmeans **kwargs

# For DBSCAN Clustering
python src/citations/scripts/optimize_clustering.py data/articles_embedded.jsonl data/clustering/clusters_DBSCAN_kwargs.json DBSCAN **kwargs

# For HDBSCAN Clustering
python src/citations/scripts/optimize_clustering.py data/articles_embedded.jsonl data/clustering/clusters_HDBSCAN_kwargs.json HDBSCAN **kwargs
```

Similarly, the script can be run with multiobjective optimization for multiple fitness functions ( silhouette_score, davies_bouldin_score, calinski_harabasz_score ) using run_mo_opt_clustering.py. 

## 7. Dimension Reduction of Embeddings
```bash
# run dimension reduction
python src/citations/scripts/run_umap.py --input_file data/articles_embedded.jsonl --output_file data/umap_results.json
```

## 8. Creating extended article

```bash
python src/citations/scripts/create_extended_article.py configs/extend_article.yml 
```


## 9. Loading extented articles to neo4j

```bash
# load into local neo4j instance
python src/citations/scripts/integrate_batch.py data/  bolt://localhost:7687 neo4j password --wipe_db --batch_size 1000

# load into AuraDB
# check the AuraDB file upon creation of instance for its link and password
python src/citations/scripts/integrate_batch.py /path/to/citation-graph/data/ neo4j+s://{instance_id}.databases.neo4j.io neo4j {password} --wipe_db --batch_size 1000
```

Note: if the authentication does not work, you can try to temporarily disable it.
In Neo4J go to 'Settings...' and set 

```bash
dbms.security.auth_enabled=false
```

However, in order to start Bloom you need to set

```bash
dbms.security.auth_enabled=true
```

To begin using Neo4j with essential features, you can explore it through [these perspectives](../../perspectives) that include tailored scene actions.


## 10. Generating keywords for articles

The process of generating keywords for articles involves several steps to ensure that the keywords are both relevant and useful for further analysis. Initially, the script `generate_keywords.py` is used to extract keywords from the clustered data. This step is computationally expensive, as it involves analyzing the text data to identify significant terms that can represent the content of each article.

Once the keywords are extracted, they are stored in `data/article_keywords.json`, which maps each article to its corresponding keywords. Additionally, the script generates `data/merge_suggestions.jsonl`, which contains suggestions for merging similar keywords. These suggestions are crucial for refining the keyword list, as they help identify terms that may be synonymous or closely related.

It is important to manually review these merge suggestions to ensure accuracy. For instance, while some terms like "spiking neural networks" may have multiple acceptable variations, others like "neurogenesis" may include terms that are not directly related, such as "neuronal differentiation." In such cases, careful curation is needed to maintain the integrity of the keyword associations.

After validation, the `process_keywords.py` script can be used to merge the keywords based on the suggestions. This script updates the keyword associations and prepares them for integration into the Neo4j database, where they can be used to enhance the semantic understanding of the articles and their relationships.

```bash
# to run extraction  (expensive)
python src/citations/scripts/topics/generate_keywords.py --json-path data/clustering/cluster_file.json 

# to force rerun keyword extraction even though output file exists (expensive)
python src/citations/scripts/topics/generate_keywords.py --json-path data/clustering/cluster_file.json --force-reextract

# to run only keyword merge suggestions
python src/citations/scripts/topics/generate_keywords.py --json-path data/clustering/cluster_file.json --force-suggest
```

Once run, the following files will be generated. 

- data/article_keywords.json: keywords per article uid

- data/merge_suggestions.jsonl: suggestions to merge similar keywords 

Suggestions should be carefully investigated if its going to be used to merge existing keywords. 

Here is a sample line which is ok so we can leave it as it is

```json
{"spiking_neural_networks": ["spiking neural networks", "Spiking Neural Networks", "spiking neuron network", "spiking neural models"]}
```

Here is one which is not good to merge for one keyword.

```json
{"neurogenesis": ["Neurogenesis", "neuronal differentiation", "neuronal birth"]}
```

we can remove neuronal differentiation as its not directly related to neurogenesis

```json
{"neurogenesis": ["Neurogenesis", "neuronal birth"]}
```

Once validated with human in the loop, we can continue merging these keywords, if desired, with the following script.

```bash
# with suggestions
python src/citations/scripts/topics/process_keywords.py --article-keywords data/article_keywords.json --clusters data/clustering/cluster_file.json --merge-suggestions data/keyword_merge_suggestions.jsonl

# without suggestions
python src/citations/scripts/topics/process_keywords.py --article-keywords data/article_keywords.json --clusters data/clustering/cluster_file.json 

# force running even output exists
python src/citations/scripts/topics/process_keywords.py --article-keywords data/article_keywords.json --clusters data/clustering/cluster_file.json --merge-suggestions data/keyword_merge_suggestions.jsonl
```

This will first read if data/cluster_results.json exists and if it does, it loads it unless we want to force it to rerun with --force-run

Then it will generate:

- data/cluster_results.json : Creates keywords and summarization of keywords per cluster
- data/updated_article_keywords.json : Same as article_keywords but with merged keywords as suggested from keyword_merge_suggestions.jsonl

It will also load these keywords as (Keyword) entity into neo4j instance, update (Topic) entity with topic summary as a property and create edge HAS_KEYWORD between (Article) and (Keyword) entities.

Then we can integrate keywords into neo4j:

```bash
python src/citations/scripts/topics/integrate_keyword_to_neo4j.py --embeddings-file data/keywords_embedded.jsonl --umap-file data/keywords_umap.json --article-keywords data/updated_article_keywords.json
```

For simplicity, neo4j uri, user and password was set to default neo4j settings. To adapt it to AuraDB , each script should be added --neo4j-uri, --neo4j-user and --neo4j-password arguments 
