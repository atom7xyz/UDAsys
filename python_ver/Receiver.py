import itertools
import json
import threading
import time
from statistics import fmean, median
from threading import Event, Semaphore
from traceback import print_exception

import zenoh

from bus import BUS

FIELDS = ["wind_dir", "wind_speed", "humidity", "temperature", "air_pressure", "height"]


class Receiver:
    def __init__(self, conf: zenoh.Config, root: str, max_buf_size: int = 10_000):
        self.___buffer = []
        self.__base_channel = root
        self.___stop = Event()
        self.___sub = threading.Thread(target=lambda: self.run_subscriber(conf), daemon=True)
        self.___querier = threading.Thread(target=lambda: self.run_querier(conf), daemon=True)
        self.___stats = threading.Thread(target=lambda: self.run_stats(conf), daemon=True)
        self.___sem = Semaphore()
        self.___sub.start()
        self.___querier.start()
        self.___stats.start()

    def ensure_size(self):
        if len(self.___buffer) > 10_000:
            self.___buffer = itertools.islice(self.___buffer, 10_000)

    def write(self, payload: str):
        self.___sem.acquire()
        self.___buffer.insert(0, payload)
        self.ensure_size()
        self.___sem.release()
        BUS.publish("receiver", self.__base_channel, payload)

    def aggregate(self, parameters: str) -> str:
        window = 0
        for kv in (parameters or "").split("&"):
            if "=" in kv:
                k, v = kv.split("=", 1)
                if k == "window":
                    try:
                        window = int(v)
                    except ValueError:
                        window = 0

        now = int(time.time())
        self.___sem.acquire()
        payloads = list(self.___buffer)
        self.___sem.release()

        rows = []
        for p in payloads:
            parts = p.split("|")
            if len(parts) != len(FIELDS) + 1:
                continue
            timestamp = int(parts[0])
            if window and timestamp < now - window:
                continue
            rows.append([float(x) for x in parts[1:]])

        result = {"channel": self.__base_channel, "window": window, "count": len(rows)}
        for i, field in enumerate(FIELDS):
            values = [r[i] for r in rows]
            if values:
                result[field] = {
                    "min": round(min(values), 2),
                    "max": round(max(values), 2),
                    "avg": round(fmean(values), 2),
                    "median": round(median(values), 2),
                }
        return json.dumps(result)

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

    def run_stats(self, config):
        with zenoh.open(config) as session:
            key = f"stats/{self.__base_channel}"
            queryable = session.declare_queryable(key)
            while not self.___stop.is_set():
                query = queryable.try_recv()
                if query is None:
                    time.sleep(0.1)
                    continue
                with query:
                    query.reply(key, self.aggregate(str(query.parameters)))

    def run_subscriber(self, config):
        with zenoh.open(config) as session:

            def listener(sample: zenoh.Sample):
                self.write(sample.payload.to_string())

            sub = session.declare_subscriber(self.__base_channel, listener)

            while not self.___stop.wait(0.2):
                pass

    def signal(self):
        self.___stop.set()

    def close(self):
        self.___stop.set()
        self.___sub.join(1)
        self.___querier.join(1)
        self.___stats.join(1)
