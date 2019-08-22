from datetime import datetime
import logging
import random
import paramiko
import constants


class ClusterSetup():
    cluster_total_nodes = constants.CLUSTER_TOTAL_NODES

    def __init__(self):
        self.service_layout, self.num_index_nodes = self.generate_cluster_topo()
        self.logger = self.initialize_logger("cluster-setup")

    def get_service_layout(self):
        return self.service_layout

    def get_num_index_nodes(self):
        return self.num_index_nodes

    def initialize_logger(self, logger_name):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        timestamp = str(datetime.now().strftime('%Y%m%dT_%H%M%S'))
        fh = logging.FileHandler("./{0}-{1}.log".format(logger_name, timestamp))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        return logger


    def install_cb_and_initialize_cluster(self):
        # SSH and uninstall couchbase and cleanup
        uninstall_cmd = "rpm -e `rpm -qa | grep couchbase-server`; rm -rf /opt/couchbase; pkill -u couchbase"
        for server in constants.SERVERS:
            self.logger.info("Uninstalling previous versions of couchbase server from {0}".format(server))
            self.execute_command(uninstall_cmd, server, constants.SERVERS_SSH_USERNAME, constants.SERVERS_SSH_PASSWORD)

        # download and install specified build
        build_url = constants.BUILD_LOCATION + constants.RELEASE_NAME + "/" + str(
            constants.BUILD_NUM) + "/" + constants.PACKAGE_NAME
        download_and_install_build_cmd = "cd /tmp; rm -rf *; wget {0}; rpm -i /tmp/{1}".format(build_url, constants.PACKAGE_NAME)
        for server in constants.SERVERS:
            self.logger.info("Downloading and installing couchbase server on {0}".format(server))
            self.execute_command(download_and_install_build_cmd, server, constants.SERVERS_SSH_USERNAME,
                                 constants.SERVERS_SSH_PASSWORD)
        # check if installed correctly

        # initialize cluster
        self.logger.info("Initializing cluster")
        cluster_init_cmd = "/opt/couchbase/bin/couchbase-cli cluster-init -c localhost --cluster-username=Administrator --cluster-password=password --cluster-port=8091 --cluster-ramsize=375 --cluster-index-ramsize=375 --cluster-name=Test "
        service_on_nodes = self.service_layout.split(":")
        cluster_init_cmd += "--services={0}".format(service_on_nodes[0].replace("+", ","))
        cmd_successful = True

        for i in range(0, len(service_on_nodes)):
            if i == 0:
                _, output, error = self.execute_command(cluster_init_cmd, constants.SERVERS[i],
                                                        constants.SERVERS_SSH_USERNAME, constants.SERVERS_SSH_PASSWORD)
                if "SUCCESS" not in str(output):
                    cmd_successful &= False
                    self.logger.error("Cluster init has an issue.")
                    self.logger.info(output)
                    self.logger.info(error)
            else:
                # Add server to existing cluster
                server_add_cmd = "/opt/couchbase/bin/couchbase-cli server-add -c {0} -u Administrator -p password --server-add={1} --server-add-username=Administrator --server-add-password=password ".format(
                    constants.SERVERS[0], constants.SERVERS[i])

                server_add_cmd += "--services={0}".format(service_on_nodes[i].replace("+", ","))
                _, output, error = self.execute_command(server_add_cmd, constants.SERVERS[i],
                                                        constants.SERVERS_SSH_USERNAME, constants.SERVERS_SSH_PASSWORD)
                if "SUCCESS" not in str(output):
                    cmd_successful &= False
                    self.logger.error("Add servers cmd has an issue.")
                    self.logger.info(output)
                    self.logger.info(error)

        cluster_rebalance_cmd = "/opt/couchbase/bin/couchbase-cli rebalance -c localhost -u Administrator  -p password"
        _, output, error = self.execute_command(cluster_rebalance_cmd, constants.SERVERS[0],
                                                constants.SERVERS_SSH_USERNAME, constants.SERVERS_SSH_PASSWORD)
        if "SUCCESS" not in str(output):
            cmd_successful &= False
            self.logger.error("Cluster rebalance has an issue.")
            self.logger.info(output)
            self.logger.info(error)

        if not cmd_successful:
            self.logger.error("Cluster setup was not successful")
        else :
            self.logger.info("*** Cluster is setup successfully ***")

    def execute_command(self, command, hostname, ssh_username, ssh_password):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, username=ssh_username, password=ssh_password,
                    timeout=120, banner_timeout=120)

        channel = ssh.get_transport().open_session()
        channel.get_pty()
        channel.settimeout(900)
        stdin = channel.makefile('wb')
        stdout = channel.makefile('rb')
        stderro = channel.makefile_stderr('rb')

        channel.exec_command(command)
        data = channel.recv(1024)
        temp = ""
        while data:
            temp += data
            data = channel.recv(1024)
        channel.close()
        stdin.close()

        output = []
        error = []
        for line in stdout.read().splitlines():
            output.append(line)
        for line in stderro.read().splitlines():
            error.append(line)
        if temp:
            line = temp.splitlines()
            output.extend(line)
        stdout.close()
        stderro.close()

        # self.logger.info("Executing on {0}: {1}".format(hostname, command))
        # ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command)

        # output = ""
        # while not ssh_stdout.channel.exit_status_ready():
        #    # Only print data if there is data to read in the channel
        #    if ssh_stdout.channel.recv_ready():
        #        rl, wl, xl = select.select([ssh_stdout.channel], [], [], 0.0)
        #        if len(rl) > 0:
        #            tmp = ssh_stdout.channel.recv(1024)
        #            output += tmp.decode()

        # output = output.split("\n")

        ssh.close()

        # return len(output) - 1, output, ssh_stderr
        return len(output), output, error

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

        # Create service layout satisfying num_index_nodes generated above
        service_layout = select_layout_pattern
        for i in range(1, num_index_nodes):
            service_layout = service_layout + ":" + select_layout_pattern

        print service_layout
        return service_layout, num_index_nodes


if __name__ == '__main__':
    clustersetup = ClusterSetup()
    #clustersetup.install_cb_and_initialize_cluster()

