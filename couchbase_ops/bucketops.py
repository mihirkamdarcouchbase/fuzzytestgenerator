import logging
import time
import urllib.parse

from couchbase.bucket import LOCKMODE_WAIT, CouchbaseError
from couchbase.cluster import Cluster, PasswordAuthenticator

from CommonUtil import constants
from CommonUtil.rest_util import RestUtil
from CommonUtil.rest_util import exec_stats


class BucketOps:

    def __init__(self):
        self.threads = constants.KV_OPS_PROCESSES
        self.spec = "couchbase://" + constants.SERVERS[0]
        self.user = "Administrator"
        self.password = "password"
        self.timeout = 600
        self.bucket_connection = None
        self.log = logging.getLogger()
        self.rest_master_conn = RestUtil(constants.SERVERS[0], constants.MASTER_PORT)
        self.num_retry_ops = 4

    def create_connection(self, bucket_name):
        """
        Create bucket connections. 5 bucket connections are created per instance.
        :return: Nothing
        """
        cluster = Cluster(self.spec)
        auth = PasswordAuthenticator(self.user, self.password)
        cluster.authenticate(auth)
        self.bucket_connection = cluster.open_bucket(bucket_name, lockmode=LOCKMODE_WAIT)
        self.bucket_connection.timeout = self.timeout

    def close_connection(self):
        self.bucket_connection.close()

    def create_bucket(self, bucket='',
                      ram_quota_mb=1,
                      replica_number=1,
                      bucket_type='membase',
                      replica_index=1,
                      threads_number=3,
                      flush_enabled=1,
                      eviction_policy='valueOnly'):

        init_params = {'name': bucket,
                       'authType': 'sasl',
                       'saslPassword': '',
                       'ramQuotaMB': ram_quota_mb,
                       'replicaNumber': replica_number,
                       'bucketType': bucket_type,
                       'replicaIndex': replica_index,
                       'threadsNumber': threads_number,
                       'flushEnabled': flush_enabled,
                       'evictionPolicy': eviction_policy}
        params = urllib.parse.urlencode(init_params)
        maxwait = 60
        for numsleep in range(maxwait):
            status, content, header = self.rest_master_conn._http_request(constants.BUCKET_URI, 'POST', params)
            if status:
                break
            elif (int(header['status']) == 503 and
                  '{"_":"Bucket with given name still exists"}' in content):
                self.log.info("The bucket still exists, sleep 1 sec and retry")
                time.sleep(1)

        if (numsleep + 1) == maxwait:
            self.log.error("Tried to create the bucket for {0} secs.. giving up".
                              format(maxwait))

    @exec_stats
    def upsert_items(self, items, ttl=0, retry=0):

        try:
            result = self.bucket_connection.upsert_multi(items, ttl=ttl, replicate_to=0)
            return result.__len__()
        except CouchbaseError as e:
            ok, fail = e.split_results()
            failed_keys = ""
            for key in fail:
                failed_keys = "{0},{1}".format(failed_keys, key)

            self.log.error("Keys failed to insert : {0}. Retrying".format(failed_keys))

            if retry != self.num_retry_ops:
                return self.upsert_items(items, ttl, retry+1)
            else:
                raise Exception("Tried 4 times, still failed: {0}".format(failed_keys))

    @exec_stats
    def delete_items(self, keys, retry=0):

        try:
            result = self.bucket_connection.remove_multi(keys)
            return result.__len__()
        except CouchbaseError as e:
            ok, fail = e.split_results()
            failed_keys = ""
            for key in fail:
                failed_keys = "{0},{1}".format(failed_keys, key)

            self.log.error("Keys failed to delete : {0}. Retrying".format(failed_keys))
            if retry != self.num_retry_ops:
                return self.delete_items(keys, retry+1)
            else:
                raise Exception("Tried 4 times, still failed: {0}".format(failed_keys))