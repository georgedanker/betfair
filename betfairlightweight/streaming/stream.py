import datetime
import logging
import time
#import queue

from .cache import MarketBookCache, OrderBookCache

logger = logging.getLogger(__name__)


class BaseStream:
    """Separate stream class to hold market/order caches
    """

    _lookup = "mc"

    def __init__(self, listener):
        self._listener = listener

        self._initial_clk = None
        self._clk = None
        self._caches = {}
        self._updates_processed = 0
        self._on_creation()

        self.time_created = datetime.datetime.utcnow()
        self.time_updated = datetime.datetime.utcnow()

    def on_subscribe(self, data):
        self._update_clk(data)
        publish_time = data.get("pt")

        if self._lookup in data:
            self._process(data[self._lookup], publish_time)
        logger.info(
            "[Stream: %s]: %s %s added"
            % (self.unique_id, len(self._caches), self._lookup)
        )

    def on_heartbeat(self, data):
        self._update_clk(data)

    def on_resubscribe(self, data):
        self.on_update(data)
        logger.info(
            "[Stream: %s]: %s %s resubscribed"
            % (self.unique_id, len(self._caches), self._lookup)
        )

    def on_update(self, data):
        self._update_clk(data)

        publish_time = data["pt"]
        latency = self._calc_latency(publish_time)
        if self._max_latency and latency > self._max_latency:
            logger.warning("[Stream: %s]: Latency high: %s" % (self.unique_id, latency))

        if self._lookup in data:
            self._process(data[self._lookup], publish_time)

    def clear_cache(self):
        self._caches.clear()

    def snap(self, market_ids=None):
        return [
            cache.create_resource(self.unique_id, None, self._lightweight)
            for cache in list(self._caches.values())
            if market_ids is None or cache.market_id in market_ids
        ]

    def on_process(self, output):
        if self.output_queue:
            self.output_queue.put(output)

    def _on_creation(self):
        logger.info('[Stream: %s]: "%s" created' % (self.unique_id, self))

    def _process(self, data, publish_time):
        pass

    def _update_clk(self, data):
        (initial_clk, clk) = (data.get("initialClk"), data.get("clk"))
        if initial_clk:
            self._initial_clk = initial_clk
        if clk:
            self._clk = clk
        self.time_updated = datetime.datetime.utcnow()

    @property
    def unique_id(self):
        return self._listener.stream_unique_id

    @property
    def output_queue(self):
        return self._listener.output_queue

    @property
    def _max_latency(self):
        return self._listener.max_latency

    @property
    def _lightweight(self):
        return self._listener.lightweight

    @staticmethod
    def _calc_latency(publish_time):
        return time.time() - publish_time / 1e3

    def __len__(self):
        return len(self._caches)

    def __str__(self):
        return "{0}".format(self.__class__.__name__)

    def __repr__(self):
        return "<{0} [{1}]>".format(self.__class__.__name__, len(self))


class MarketStream(BaseStream):

    _lookup = "mc"

    def _process(self, data, publish_time):
        output_market_book = []
        for market_book in data:
            market_id = market_book["id"]
            market_book_cache = self._caches.get(market_id)

            if (
                market_book.get("img") or market_book_cache is None
            ):  # historic data does not contain img
                if "marketDefinition" not in market_book:
                    logger.error(
                        "[MarketStream: %s] Unable to add %s to cache due to marketDefinition "
                        "not being present (make sure EX_MARKET_DEF is requested)"
                        % (self.unique_id, market_id)
                    )
                    continue
                market_book_cache = MarketBookCache(
                    publish_time=publish_time, **market_book
                )
                self._caches[market_id] = market_book_cache
                logger.info(
                    "[MarketStream: %s] %s added, %s markets in cache"
                    % (self.unique_id, market_id, len(self._caches))
                )

            market_book_cache.update_cache(market_book, publish_time)
            self._updates_processed += 1

            output_market_book.append(
                market_book_cache.create_resource(
                    self.unique_id, market_book, self._lightweight
                )
            )
        self.on_process(output_market_book)


class OrderStream(BaseStream):

    _lookup = "oc"

    def _process(self, data, publish_time):
        output_order_book = []
        for order_book in data:
            market_id = order_book["id"]
            order_book_cache = self._caches.get(market_id)

            if order_book_cache is None:
                order_book_cache = OrderBookCache(
                    publish_time=publish_time, **order_book
                )
                self._caches[market_id] = order_book_cache
                logger.info(
                    "[OrderStream: %s] %s added, %s markets in cache"
                    % (self.unique_id, market_id, len(self._caches))
                )

            order_book_cache.update_cache(order_book, publish_time)
            self._updates_processed += 1

            output_order_book.append(
                order_book_cache.create_resource(
                    self.unique_id, order_book, self._lightweight
                )
            )
        self.on_process(output_order_book)
