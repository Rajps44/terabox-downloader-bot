import logging
import os
import sys
import threading
import typing
from typing import Any
from urllib.parse import urlparse

from redis import Redis as r

log = logging.getLogger("telethon")


class Redis(r):

    def __init__(
        self,
        host: str = None,
        port: int = None,
        password: str = None,
        logger=log,
        encoding: str = "utf-8",
        decode_responses=True,
        **kwargs,
    ):
        # Check for REDIS_URL
        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            parsed_url = urlparse(redis_url)
            host = parsed_url.hostname
            port = parsed_url.port
            password = parsed_url.password
        else:
            # Fallback to provided host, port, and password
            if ":" in host:
                data = host.split(":")
                host = data[0]
                port = int(data[1])

            if not host or not port:
                logger.error("Port Number not found")
                sys.exit()

        if host.startswith("http"):
            logger.error("Your REDIS_URI should not start with http!")
            sys.exit()

        # Assign kwargs for Redis connection
        kwargs["host"] = host
        kwargs["port"] = port
        kwargs["password"] = password if password and len(password) > 1 else None
        kwargs["encoding"] = encoding
        kwargs["decode_responses"] = decode_responses

        try:
            super().__init__(**kwargs)
        except Exception as e:
            logger.exception(f"Error while connecting to redis: {e}")
            sys.exit()
        
        self.logger = logger
        self._cache = {}
        threading.Thread(target=self.re_cache).start()

    def re_cache(self):
        key = self.keys()
        for keys in key:
            self._cache[keys] = self.get(keys)
        self.logger.info("Cached {} keys".format(len(self._cache)))

    def get_key(self, key: Any):
        if key in self._cache:
            return self._cache[key]
        else:
            data = self.get(key)
            self._cache[key] = data
            return data

    def del_key(self, key: Any):
        if key in self._cache:
            del self._cache[key]
        return self.delete(key)

    def set_key(self, key: Any = None, value: Any = None):
        self._cache[key] = value
        return self.set(key, value)


# Initialize Redis
db = Redis(
    host=os.getenv("HOST", HOST), # Use env variable or fallback
    port=os.getenv("PORT", PORT),
    password=os.getenv("PASSWORD", PASSWORD) if len(PASSWORD) > 1 else None,
    decode_responses=True,
)

log.info(f"Starting redis on {HOST}:{PORT}")
if not db.ping():
    log.error(f"Redis is not available on {HOST}:{PORT}")
    exit(1)
