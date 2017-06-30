import re

def compute_contribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parse_neighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]



class pageRank():
    def __init__(self, follow_file, uid_2_index_file, num_iters):
        self.follow_file = follow_file
        self.uid_2_index_file = uid_2_index_file
        self.num_iters = num_iters


    def run(self, sc):
        ranks = sc.textFile(self.uid_2_index_file).map(lambda x: (x[0],0))
        rdd_neighbors =  sc.textFile(self.follow_file).map(lambda x: re.split(r'\s+', x)).map(lambda x: (x[0], x[1].split(',')))

        for iteration in range(self.num_iters):
            # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            # Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

            # Collects all URL ranks and dump them to console.
        for (link, rank) in ranks.collect():
            print("%s has rank: %s." % (link, rank))





