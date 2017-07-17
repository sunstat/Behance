import re
from operator import add
import os
from subprocess import call
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType

import ConfigurationFiles.constants as C

class PageRank():
    def __init__(self, num_iters):
        self.num_iters = num_iters

    @staticmethod
    def print_rdd_to_file(rdd, output_file, output_format):
        def to_string(x):
            if not x:
                return " "
            return ",".join([str(y) for y in x])

        delete_shell_azure = os.path.join(C.C.SHELL_DIR, 'delete.sh')
        call('{} {} {}'.format("/usr/bin/env bash", delete_shell_azure, output_file), shell=True)

        if os.system("hadoop fs -test -d {0}".format(output_file)) == 0:
            raise Exception("Folder already exists!!")

        # time.sleep(1)
        if output_format == 'csv':
            rdd.map(lambda x: to_string(x)).saveAsTextFile(output_file)
        elif output_format == 'psv':
            rdd.map(lambda x: str(x[0]) + "#" + (to_string(x[1]))).saveAsTextFile(output_file)


    @staticmethod
    def compute_contribs(source_url, urls, rank):
        #Calculates URL contributions to the rank of other URLs.
        if not urls == 0:
            yield (source_url, 0)
        else:
            num_urls = len(urls)
            for url in urls:
                yield (url, rank / num_urls)

    @staticmethod
    def parse_neighbors(urls):
        """Parses a urls pair string into urls pair."""
        parts = re.split('#', urls)
        return parts[0], parts[1]

    def run(self, sc, month):
        cur_dir = os.path.join(C.INTERMEDIATE_RESULT_DIR, month)

        ranks = sc.textFile(os.path.join(cur_dir, C.UID_2_INDEX_FILE)).map(lambda x: x.split(',')).map(lambda x: (x[0], 1))
        links = sc.textFile(os.path.join(cur_dir, C.FOLLOW_MAP_FILE)).map(lambda x: re.split('#', x))\
            .map(lambda x: (x[0], x[1].split(',')))
        pid_2_uid = sc.textFile(C.PID_2_UID_FILE).map(lambda x: x.split(','))
        print(links.take(5))

        for iteration in range(self.num_iters):
            # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: PageRank.compute_contribs(url_urls_rank[0], url_urls_rank[1][0], url_urls_rank[1][1]))
            # Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(add).mapValues(lambda x: x * 0.85 + 0.15)

            # Collects all URL ranks and dump them to console.
        pid_2_score = pid_2_uid.join(ranks).map(lambda x: (x[0], x[1][1]))
        output_file = os.path.join(cur_dir, "pid_2_score-csv")
        PageRank.print_rdd_to_file(pid_2_score,  output_file, 'csv')





