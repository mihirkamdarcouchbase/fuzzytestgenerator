import logging
import logging.config
import logging.handlers
import paramiko
import yaml


def execute_command(command, hostname, ssh_username, ssh_password, logger):
    logger.info("Running {0} on host {1}".format(command, hostname))
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
        temp += str(data)
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

    ssh.close()
    return len(output), output, error


def initialize_logger(logger_name):
    logger = logging.getLogger(logger_name)
    config = yaml.load(open('CommonUtil/log_config.yaml').read(), Loader=yaml.Loader)
    config["handlers"]["info_file_handler"]["fname"] = "{0}-info".format(logger_name)
    config["handlers"]["error_file_handler"]["fname"] = "{0}-error".format(logger_name)
    config["handlers"]["critical_file_handler"]["fname"] = "{0}-critical".format(logger_name)
    logging.config.dictConfig(config)

    return logger