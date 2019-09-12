from datagenerator import SchemaGenerator
from couchbase_ops.clustersetup import ClusterSetup
from couchbase_ops.bucketops import BucketOps
from couchbase_ops.indexgenerator import IndexGenerator
from datagenerator import IntiateDataGenerator
from CommonUtil import constants
import time


class FuzzyTestGenerator():

    def __init__(self):
        clustersetup = ClusterSetup()
        clustersetup.install_cb_and_initialize_cluster()
        bucket_ops = BucketOps()
        bucket_ops.create_bucket(constants.BUCKET_NAME, 300)
        time.sleep(10)
        num_index_nodes = clustersetup.get_num_index_nodes()
        schema = SchemaGenerator().get_schema()
        batch_meta = {
            "random_key": False,
            "schema": schema,
            "UPSERT": {
                "DOCS": 0.2,
                "RANDOM": False
            },
            "DELETE": {
                "DOCS": 0.3,
                "RANDOM": False
            },
            "EXPIRY": {
                "DOCS": 0.3,
                "TIME": 10000
            }
        }
        IntiateDataGenerator(10000000, batch_meta).initiate()

        IndexGenerator(schema, num_index_nodes)


if __name__ == '__main__':
    schema = FuzzyTestGenerator()
