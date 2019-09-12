import sys
import logging
import traceback
import threading
import multiprocessing
from logging import FileHandler as FH
from datetime import datetime


# ============================================================================
# Define Log Handler
# ============================================================================
class CustomLogHandler(logging.Handler):
    """multiprocessing log handler

    This handler makes it possible for several processes
    to log to the same file by using a queue.

    """
    def __init__(self, fname):
        logging.Handler.__init__(self)
        timestamp = str(datetime.now().strftime('%Y%m%dT_%H%M%S'))

        self._handler = FH("logs/{0}-{1}.log".format(fname, timestamp))
        self.queue = multiprocessing.Queue(-1)

        thrd = threading.Thread(target=self.receive)
        thrd.daemon = True
        thrd.start()

    def setFormatter(self, fmt):
        logging.Handler.setFormatter(self, fmt)
        self._handler.setFormatter(fmt)

    def receive(self):
        while True:
            try:
                record = self.queue.get()
                self._handler.emit(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except:
                traceback.print_exc(file=sys.stderr)

    def send(self, s):
        self.queue.put_nowait(s)

    def _format_record(self, record):
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            dummy = self.format(record)
            record.exc_info = None

        return record

    def emit(self, record):
        try:
            s = self._format_record(record)
            self.send(s)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        self._handler.close()
        logging.Handler.close(self)


class LoggingErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.ERROR


class LoggingInfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO or record.levelno == logging.CRITICAL


class LoggingCriticalFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.CRITICAL
