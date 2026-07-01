import queue
import time
from threading import Semaphore


class EventBus:
    def __init__(self):
        self.___subscribers = []
        self.___sem = Semaphore()

    def subscribe(self) -> queue.Queue:
        q = queue.Queue()
        self.___sem.acquire()
        self.___subscribers.append(q)
        self.___sem.release()
        return q

    def unsubscribe(self, q: queue.Queue):
        self.___sem.acquire()
        if q in self.___subscribers:
            self.___subscribers.remove(q)
        self.___sem.release()

    def publish(self, kind: str, channel: str, payload: str):
        event = {
            "kind": kind,
            "channel": channel,
            "payload": payload,
            "timestamp": int(time.time()),
        }
        self.___sem.acquire()
        for q in self.___subscribers:
            q.put(event)
        self.___sem.release()


BUS = EventBus()
