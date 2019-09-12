import random
from CommonUtil import constants, util
import itertools
import time
import concurrent.futures


class ClusterSetup:
    cluster_total_nodes = constants.CLUSTER_TOTAL_NODES

    def __init__(self):
        self.log = util.initialize_logger("cluster-setup")
        self.service_layout, self.num_index_nodes = self.generate_cluster_topo()
        self.service_on_nodes = self.service_layout.split(":")
        self.num_nodes_required = len(self.service_on_nodes)
        self.nodes = constants.SERVERS[:self.num_nodes_required]
        self.master_node = self.nodes.pop(-1)

    def get_service_layout(self):
        return self.service_layout

    def get_num_index_nodes(self):
        return self.num_index_nodes

    def install_cb_and_initialize_cluster(self):
        # SSH and uninstall couchbase and cleanup
        uninstall_cmd = "rpm -e `rpm -qa | grep couchbase-server`; rm -rf /opt/couchbase; pkill -u couchbase"
        with concurrent.futures.ProcessPoolExecutor(max_workers=100) as executor:
            for server in constants.SERVERS:
                self.log.info("Uninstalling previous versions of couchbase server from {0}".format(server))
                executor.submit(util.execute_command, uninstall_cmd, server, constants.SERVERS_SSH_USERNAME, constants.SERVERS_SSH_PASSWORD,
                                     self.log)

        # download and install specified build
        build_url = constants.BUILD_LOCATION + constants.RELEASE_NAME + "/" + str(
            constants.BUILD_NUM) + "/" + constants.PACKAGE_NAME
        download_and_install_build_cmd = "cd /tmp; rm -rf *; wget {0}; rpm -i /tmp/{1}".format(build_url,
                                                                                               constants.PACKAGE_NAME)
        with concurrent.futures.ProcessPoolExecutor(max_workers=100) as executor:
            for server in itertools.chain([self.master_node], self.nodes):
                self.log.info("Downloading and installing couchbase server on {0}".format(server))
                executor.submit(util.execute_command, download_and_install_build_cmd, server, constants.SERVERS_SSH_USERNAME,
                                 constants.SERVERS_SSH_PASSWORD, self.log)

        time.sleep(10)
        # check if installed correctly

        # initialize cluster
        self.log.info("Initializing cluster")
        print(self.master_node)
        cmd_successful = self.init_cluster("Test", self.master_node, self.service_on_nodes.pop(-1))

        for node in self.nodes:
            print(node)
            # Add server to existing cluster
            server_add_cmd = "/opt/couchbase/bin/couchbase-cli server-add -c {0} -u {1} -p {2} " \
                             "--server-add=http://{3} --server-add-username={4} --server-add-password={5} " \
                             "".format(self.master_node, constants.REST_USERNAME, constants.REST_PASSWORD, node,
                                       constants.REST_USERNAME, constants.REST_PASSWORD)

            server_add_cmd += "--services={0}".format(self.service_on_nodes.pop().replace("+", ","))
            _, output, error = util.execute_command(server_add_cmd, node,
                                                    constants.SERVERS_SSH_USERNAME, constants.SERVERS_SSH_PASSWORD,
                                                    self.log)
            if "SUCCESS" not in str(output):
                cmd_successful &= False
                self.log.error("Add servers cmd has an issue.")
                self.log.info(output)
                self.log.info(error)

        cluster_rebalance_cmd = "/opt/couchbase/bin/couchbase-cli rebalance -c localhost -u Administrator  -p password"
        _, output, error = util.execute_command(cluster_rebalance_cmd, constants.SERVERS[0],
                                                constants.SERVERS_SSH_USERNAME, constants.SERVERS_SSH_PASSWORD,
                                                self.log)
        if "SUCCESS" not in str(output):
            cmd_successful &= False
            self.log.error("Cluster rebalance has an issue.")
            self.log.info(output)
            self.log.info(error)

        if not cmd_successful:
            self.log.error("Cluster setup was not successful")
        else:
            self.log.info("*** Cluster is setup successfully ***")

    def init_cluster(self, cluster_name, node, services):
        cluster_init_cmd = "/opt/couchbase/bin/couchbase-cli cluster-init -c localhost " \
                           "--cluster-username=Administrator --cluster-password=password --cluster-port=8091 " \
                           "--cluster-ramsize=375 --cluster-index-ramsize=375 --cluster-name={0}".format(cluster_name)
        cluster_init_cmd += " --services={0}".format(services.replace("+", ","))
        cmd_successful = True

        _, output, error = util.execute_command(cluster_init_cmd, node,
                                                constants.SERVERS_SSH_USERNAME, constants.SERVERS_SSH_PASSWORD,
                                                self.log)
        if "SUCCESS" not in str(output):
            cmd_successful = False
            self.log.error("Cluster init has an issue.")
            self.log.info(output)
            self.log.info(error)

        return cmd_successful

    def generate_cluster_topo(self):

        # Node layout patterns
        node_layout_patterns = constants.NODE_LAYOUT_PATTERNS

        # Generate no. of indexer nodes required
        num_index_nodes = random.randint(1, self.cluster_total_nodes)

        # Now find a service layout pattern that satisfies the num_index_nodes
        min_nodes_reqd = abs(self.cluster_total_nodes / num_index_nodes)
        allowed_pattern_list = []
        for pattern in node_layout_patterns:
            if pattern["min_nodes"] <= min_nodes_reqd:
                allowed_pattern_list.append(pattern["pattern"])

        # Select one layout pattern from the allowed list
        select_layout_pattern = random.choice(allowed_pattern_list)
        self.log.info("selected layout pattrens : {0}".format(select_layout_pattern))

        # Create service layout satisfying num_index_nodes generated above
        service_layout = select_layout_pattern
        for i in range(1, num_index_nodes):
            service_layout = service_layout + ":" + select_layout_pattern

        print(service_layout)
        return service_layout, num_index_nodes


if __name__ == '__main__':
    clustersetup = ClusterSetup()
    # clustersetup.install_cb_and_initialize_cluster()
