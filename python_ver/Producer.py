import random
import threading
import time
from itertools import product
from typing import Optional

import numpy
import zenoh

from bus import BUS


def main(config: zenoh.Config):
    import Receiver
    zenoh.init_log_from_env_or("error")
    map = {
        "sardegna": {
            "cagliari": 5,
            "sassari": 3,
            "oristano": 2
        }
    }

    producers = []
    receivers = []

    for k in map.keys():
        for k2 in map[k].keys():
            receivers.append(Receiver.Receiver(config, f"{k}/{k2}"))
            for v in range(map[k][k2]):
                producers.append(create_producer(config, f"{k}/{k2}"))

    return producers, receivers



def create_producer(con: zenoh.Config, root: str):
    return Producer(con, root)


class Product:
    ___RNG = numpy.random.default_rng(None)

    def __init__(self, wind_dir, wind_speed, humidity, temperature, air_pressure, height):
        self.___wind_dir = wind_dir
        self.___wind_speed = wind_speed
        self.___humidity = humidity
        self.___temperature = temperature
        self.___air_pressure = air_pressure
        self.___height = height

    def product(self):
        timestamp = int(time.time())
        wind_dir = Product.___RNG.normal(self.___wind_dir, 1.0, 1).take(0)
        wind_speed = Product.___RNG.normal(self.___wind_speed, 1.0, 1).take(0)
        humidity = Product.___RNG.normal(self.___humidity, 1.0, 1).take(0)
        temperature = Product.___RNG.normal(self.___temperature, 1.0, 1).take(0)
        air_pressure = Product.___RNG.normal(self.___air_pressure, 1.0, 1).take(0)

        return f"{timestamp}|{wind_dir:.2f}|{wind_speed:.2f}|{humidity:.2f}|{temperature:.2f}|{air_pressure:.2f}|{self.___height}"

class Producer:
    def __init__(self, conf: zenoh.Config, root: str, id = [0]):
        self.__base_channel = root
        self.__channel = f"{root}/{id[0]}"
        id[0] += 1

        random.seed(id[0])

        self.___product = Product(
            random.randrange(0, 360),
            random.uniform(0.5, 40.),
            random.randrange(0, 100),
            random.uniform(5.0, 45.0),
            random.randrange(870, 1085),
            random.randrange(0, 1_834)
        )
        self.___pub = threading.Thread(target=lambda: self.run_pub(conf), daemon=True)
        self.___query = threading.Thread(target=lambda: self.run_queryable(conf), daemon=True)
        self.___pub.start()
        self.___query.start()

    def channel(self):
        return self.__channel

    def run_pub(self, config):
        with zenoh.open(config) as session:
            pub = session.declare_publisher(self.__base_channel)
            while True:
                time.sleep(1)
                payload = self.___product.product()
                pub.put(payload)
                BUS.publish("producer", self.__channel, payload)

    def run_queryable(self, config):
        with zenoh.open(config) as session:
            queryable = session.declare_queryable(self.__channel)
            while True:
                with queryable.recv() as query:
                    # selector = query.selector
                    # payload = query.payload
                    query.reply(self.__channel, self.___product.product())

    def close(self):
        self.___pub.join(0)
        self.___query.join(0)


if __name__ == "__main__":
    import argparse
    import itertools

    import common

    parser = argparse.ArgumentParser(prog="z_pub", description="zenoh pub example")
    common.add_config_arguments(parser)
    args = parser.parse_args()
    conf = common.get_config_from_args(args)
    main(conf)