"""Tests for article embeddings."""


def test_articles_in_clusters(articles, clusterings):
    """Ensure that each article has a cluster."""
    for clustering_file, clustering in clusterings.items():
        # Hdbscan intentionally excludes some samples
        if clustering_file == "clusters_hdbscan.json":
            continue

        all_cluster_uids = []
        clusters = clustering["clusters"]
        for cluster in clusters.values():
            all_cluster_uids.extend(cluster)

        for _, row in articles.iterrows():
            assert row.uid in all_cluster_uids, (
                f"In clustering {clustering_file}: article {row.uid} not in"
                " any clusters."
            )


def test_clusters_articles_exist(articles, clusterings):
    """Ensure that all articles in clusters exist."""
    article_uids = articles["uid"].values
    for clustering_file, clustering in clusterings.items():
        clusters = clustering["clusters"]
        for cluster in clusters.values():
            for cluster_article in cluster:
                assert cluster_article in article_uids, (
                    f"In clustering {clustering_file}: article"
                    f" {cluster_article} is not in articles.csv."
                )
