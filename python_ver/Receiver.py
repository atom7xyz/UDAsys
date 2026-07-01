import itertools
import threading
import time
from threading import Semaphore
from traceback import print_exception

import zenoh


class Receiver:
    def __init__(self, conf: zenoh.Config, root: str, max_buf_size: int = 10_000):
        self.___buffer = []
        self.__base_channel = root
        self.___sub = threading.Thread(target=lambda: self.run_subscriber(conf))
        self.___querier = threading.Thread(target=lambda: self.run_querier(conf))
        self.___sem = Semaphore()
        self.___sub.start()
        self.___querier.start()
        print(self.__base_channel)

    def ensure_size(self):
        if len(self.___buffer) > 10_000:
            self.___buffer = itertools.islice(self.___buffer, 10_000)

    def write(self, payload: str):
        self.___sem.acquire()
        self.___buffer.append(payload)
        self.ensure_size()
        print(f"{self.__base_channel}: {payload}")
        self.___sem.release()

    def run_querier(self, config):
        with zenoh.open(config) as session:
            query_selector = zenoh.Selector(f"{self.__base_channel}/**")
            querier = session.declare_querier(query_selector.key_expr)
            parameters = ""
            for reply in querier.get(parameters=parameters):
                if ro := reply.ok:
                    self.write(ro.payload.to_string())
                else:
                    print_exception(Exception(reply.err.payload.to_string()))


    def run_subscriber(self, config):
        with zenoh.open(config) as session:

            def listener(sample: zenoh.Sample):
                self.write(sample.payload.to_string())

            sub = session.declare_subscriber(self.__base_channel, listener)

            while True:
                time.sleep(1)

    def close(self):
        self.___sub.join(0)
        self.___querier.join(0)