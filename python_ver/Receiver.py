import itertools
import threading
import time
from threading import Event, Semaphore
from traceback import print_exception

import zenoh

from bus import BUS


class Receiver:
    def __init__(self, conf: zenoh.Config, root: str, max_buf_size: int = 10_000):
        self.___buffer = []
        self.__base_channel = root
        self.___stop = Event()
        self.___sub = threading.Thread(target=lambda: self.run_subscriber(conf), daemon=True)
        self.___querier = threading.Thread(target=lambda: self.run_querier(conf), daemon=True)
        self.___sem = Semaphore()
        self.___sub.start()
        self.___querier.start()

    def ensure_size(self):
        if len(self.___buffer) > 10_000:
            self.___buffer = itertools.islice(self.___buffer, 10_000)

    def write(self, payload: str):
        self.___sem.acquire()
        self.___buffer.insert(0, payload)
        self.ensure_size()
        self.___sem.release()
        BUS.publish("receiver", self.__base_channel, payload)

    def run_querier(self, config):
        with zenoh.open(config) as session:
            query_selector = zenoh.Selector(f"{self.__base_channel}/**")
            querier = session.declare_querier(query_selector.key_expr)
            parameters = ""
            for reply in querier.get(parameters=parameters):
                if self.___stop.is_set():
                    return
                if ro := reply.ok:
                    self.write(ro.payload.to_string())
                else:
                    print_exception(Exception(reply.err.payload.to_string()))

    def run_subscriber(self, config):
        with zenoh.open(config) as session:

            def listener(sample: zenoh.Sample):
                self.write(sample.payload.to_string())

            sub = session.declare_subscriber(self.__base_channel, listener)

            while not self.___stop.is_set():
                time.sleep(0.2)

    def close(self):
        self.___stop.set()
        self.___sub.join(1)
        self.___querier.join(1)
