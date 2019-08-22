import random
import string
import logging
from datetime import datetime
from datagenerator import SchemaGenerator
from clustersetup import ClusterSetup
from indexgenerator import IndexGenerator

class FuzzyTestGenerator():

    def __init__(self):
        clustersetup = ClusterSetup()
        #clustersetup.install_cb_and_initialize_cluster()
        num_index_nodes = clustersetup.get_num_index_nodes()
        self.schema = SchemaGenerator(10).get_schema()

        IndexGenerator(self.schema, num_index_nodes)

if __name__ == '__main__':
    schema = FuzzyTestGenerator()


