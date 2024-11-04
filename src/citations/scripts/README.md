# Description

Below is the pipeline to generate citation graph and load it into neo4j. Please read the notes at the end first if you want to use it with AuraDB.

## Gathering Authors and Articles

### 1a. Fetch article data from EuroPMC 

given bbp article get its metada from europmc in xml (5714 articles)
TBW

### 1b. Fetch author info from obtained articles using ORCID api

TBW

### 1b. Use BBP articles to fetch citations from Google Scholar


Run 
python src/citations/scripts/combine_serp.py --input {path/to/serp_citation_results.jsonl} --output {path_to_output} --data_dir data/ 

which will update articles.csv authors.csv author_wrote_article.csv article_cites_article.csv with SERP api data.
The institutions wont be updated as this field is not available for google scholar directly.

Dev notes:

If europmc doesnt have the data, use serp to fetch the same metadata. (150 articles according to folder content from s3 but neo4j had different number)

serp api is given bbp articles only if that articles data is not fetched with EuroPMC result where euroPMC should have created a xml file stored in amazon s3 sandbox. 

bugs:
- julian and davide doesnt look red (bbp author)

## 1c. manual from csv

if both cases dont work because article is not published etc, we create Article entity manually from whatever data is available.

First you will need to fetch the publication data from the S3 bucket.

You can find it here:

https://us-east-1.console.aws.amazon.com/s3/buckets/citation-project-dvc?region=us-east-1&bucketType=general&tab=objects

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

## Embed articles with their title + abtract values

```bash
python src/citations/scripts/embed_openai.py --data_dir /path/to/citation-graph/data 
```

## Cluster articles from their embeddings

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

## Dimension Reduction of Embeddings
```bash
# run dimension reduction
python src/citations/scripts/run_umap.py --input_file data/articles_embedded.jsonl --output_file data/umap_results.json
```

## Creating extended article

```bash
python src/citations/scripts/create_extended_article.py configs/extend_article.yml 
```


## Loading extented articles to neo4j

```bash
# load into local neo4j instance
python src/citations/scripts/integrate_batch.py data/  bolt://localhost:7687 neo4j password --wipe_db --batch_size 1000

# load into AuraDB
# check the AuraDB file upon creation of instance for its link and password
python src/citations/scripts/integrate_batch.py /path/to/citation-graph/data/ neo4j+s://{instance_id}.databases.neo4j.io neo4j {password} --wipe_db --batch_size 1000
```

## Generate keywords

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

##  Process keywords with suggestions jsonl and load it into neo4j

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

## Integrate keywords into neo4j 

```bash
python src/citations/scripts/topics/integrate_keyword_to_neo4j.py --embeddings-file data/keywords_embedded.jsonl --umap-file data/keywords_umap.json --article-keywords data/updated_article_keywords.json
```

## Note: 

For simplicity, neo4j uri, user and password was set to default neo4j settings. To adapt it to AuraDB , each script should be added --neo4j-uri, --neo4j-user and --neo4j-password arguments 