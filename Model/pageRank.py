import re
class pageRank():
    def __init__(self, follow_file, uid_2_index_file, num_iters):
        self.follow_file = follow_file
        self.uid_2_index_file = uid_2_index_file
        self.num_iters = num_iters


    def run(self, sc):
        def computeContribs(urls, rank):
            """Calculates URL contributions to the rank of other URLs."""
            num_urls = len(urls)
            for url in urls:
                yield (url, rank / num_urls)

        rdd_neighbors_base = sc.textFile(self.uid_2_index_file).map(lambda x: (x[0],[]))
        rdd_neighbors =  sc.textFile(self.follow_file).map(lambda x: x.split('\t')).map(lambda x: (x[0], x[1].split(",")))
        rdd_neighbors = rdd_neighbors.union(rdd_neighbors_base).distinct().reduceByKey(lambda x, y : x+y)
        links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
        ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

        for iteration in range(self.num_iters):
            # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            # Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

            # Collects all URL ranks and dump them to console.
        for (link, rank) in ranks.collect():
            print("%s has rank: %s." % (link, rank))





