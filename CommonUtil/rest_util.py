import logging
import httplib2
import time
import json
import socket
import traceback
import base64
from decorator import decorator
log = logging.getLogger()

@decorator
def exec_stats(method, *args):
    self = args[0]
    start_time = time.time()
    return_value = method(self, *args[1:])
    end_time = time.time()
    exec_time = end_time - start_time
    self.log.info("exec_time : {0}".format(exec_time))
    return return_value


class RestUtil:
    def __init__(self, hostname, port):
        self.user = "Administrator"
        self.password = "password"
        self.hostname = hostname
        self.port = port
        self.baseUrl = "http://{0}:{1}/".format(self.hostname, self.port)
        self.log = logging.getLogger()

        # authorization must be a base64 string of username:password

    def _create_headers(self):
        authorization = base64.encodestring(('%s:%s' % (self.user,self.password)).encode()).decode().replace('\n', '')
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    def _get_auth(self, headers):
        key = 'Authorization'
        if key in headers:
            val = headers[key]
            if val.startswith("Basic "):
                return "auth: " + base64.decodestring(val[6:])
        return ""

    def _http_request(self, uri, method='GET', params='', headers=None, timeout=120):
        api = '{0}{1}'.format(self.baseUrl, uri)
        if not headers:
            headers = self._create_headers()
        end_time = time.time() + timeout
        self.log.info("Executing {0} request for following api {1} with Params: {2}  and Headers: {3}" \
                       .format(method, api, params, headers))
        count = 1
        while True:
            try:
                response, content = httplib2.Http(timeout=timeout).request(api, method,
                                                                           params, headers)
                if response['status'] in ['200', '201', '202']:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                        self.log.info(json_parsed)
                    except ValueError as e:
                        json_parsed = {}
                        json_parsed["error"] = "status: {0}, content: {1}" \
                            .format(response['status'], content)
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    message = '{0} {1} body: {2} headers: {3} error: {4} reason: {5} {6} {7}'. \
                        format(method, api, params, headers, response['status'], reason,
                               content.rstrip('\n'), self._get_auth(headers))
                    log.error(message)
                    self.log.info(''.join(traceback.format_stack()))
                    return False, content, response
            except socket.error as e:
                if count < 4:
                    log.error("socket error while connecting to {0} error {1} ".format(api, e))
                if time.time() > end_time:
                    log.error("Tried ta connect {0} times".format(count))
            except httplib2.ServerNotFoundError as e:
                if count < 4:
                    log.error("ServerNotFoundError error while connecting to {0} error {1} " \
                                   .format(api, e))
                if time.time() > end_time:
                    log.error("Tried ta connect {0} times".format(count))

            time.sleep(3)
            count += 1