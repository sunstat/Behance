import re
from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType

class PageRank():
    def __init__(self, follow_file, uid_2_index_file, num_iters):
        self.follow_file = follow_file
        self.uid_2_index_file = uid_2_index_file
        self.num_iters = num_iters

    @staticmethod
    def compute_contribs(urls, rank):
        """Calculates URL contributions to the rank of other URLs."""
        num_urls = len(urls)
        for url in urls:
            yield (url, rank / num_urls)

    @staticmethod
    def parse_neighbors(urls):
        """Parses a urls pair string into urls pair."""
        parts = re.split(r'\s+', urls)
        return parts[0], parts[1]

    def run(self, sc):
        ranks = sc.textFile(self.uid_2_index_file).map(lambda x: (x[0], 0))
        links =  sc.textFile(self.follow_file).map(lambda x: re.split('#', x))\
            .map(lambda x: (x[0], x[1].split(',')))
        print(links.take(5))
        incoming_nodes = links.flatMap(lambda x: x[1]).distinct()
        print("==================")
        print(ranks.take(5))
        print(ranks.count())
        print(incoming_nodes.take(5))
        print(incoming_nodes.count())
        print("=================")

        '''
        for iteration in range(self.num_iters):
            # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: PageRank.compute_contribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            # Re-calculates URL ranks based on neighbor contributions.
            ranks_temp = contribs.reduceByKey(add).mapValues(lambda x: x * 0.85 + 0.15)

            # Collects all URL ranks and dump them to console.
        for (link, rank) in ranks.collect():
            print("%s has rank: %s." % (link, rank))
        '''



