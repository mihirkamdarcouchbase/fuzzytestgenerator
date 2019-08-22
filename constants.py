SERVERS = ["10.112.193.101","10.112.193.102","10.112.193.103","10.112.193.104"]
SERVERS_SSH_USERNAME = "root"
SERVERS_SSH_PASSWORD = "couchbase"
BUILD_LOCATION = "http://172.23.120.24/builds/latestbuilds/couchbase-server/"
RELEASE_NAME = "mad-hatter"
BUILD_NUM=2715
PACKAGE_NAME="couchbase-server-enterprise-6.5.0-{0}-centos7.x86_64.rpm".format(BUILD_NUM)
CLUSTER_TOTAL_NODES = 4
NODE_LAYOUT_PATTERNS = [
            {"pattern": "data+query+index", "min_nodes": 1},
            {"pattern": "data:query+index", "min_nodes": 2},
            {"pattern": "data+query:index", "min_nodes": 2},
            {"pattern": "data:query:index", "min_nodes": 3}
        ]
# Per node RAM in MB
MAX_RAM_PER_NODE = 23944
MAX_RAM_PERCENT_UTILIZATION = 85
MAX_NUM_INDEX_PER_NODE = 10
MAX_FIELDS_PER_INDEX = 3

META_FIELDS = ["meta().id", "meta().cas", "meta().expiration"]
BUCKET_NAME = "default"
