# -*- coding: utf-8 -*-
import random
import threading, time

import requests


class Piuka(object):
    """
        Create a PiukaEngine object

        :proxies -> dict: Proxies URI, like {"http": "http://localhost"}, see the doc for module "requests".
        :headers -> dict: HTTP headers, like {"referer": "https://www.ghink.net"}, see the doc for module "requests".
        :thread  -> int : Thread num limit. Default value is you cpu core num.
        :timeout -> int : Time limit for a single request, in seconds. Default value is 2.
        :flush   -> int : Threshold of data written to disk in Byte(s). Default value is 100MB.
    """

    def __init__(self, **kwargs):
        # Consts
        self.__VERSION = ("Alpha", 1, 0, 0)

        # Get proxies
        self.__proxies = kwargs.get("proxies", None)
        assert type(self.__proxies) == dict or self.__proxies is None

        # Get headers
        ua = "Piuka{}/{}.{}.{}".format(
            self.__VERSION[0], self.__VERSION[1], self.__VERSION[2], self.__VERSION[3]
        )
        self.__headers = kwargs.get("headers", {"user-agent": ua})
        assert type(self.__headers) == dict
        # Use lowercase uniformly
        for key, value in self.__headers.items():
            self.__headers[key.lower()] = value
            if key.lower() != key:
                del self.__headers[key]
        # Custom UA
        if "user-agent" not in self.__headers:
            self.__headers["user-agent"] = ua

        # Get timeout
        self.__timeout = kwargs.get("timeout", 2)
        assert type(self.__timeout) == int

        # Get thread num limit
        self.__thread_num = kwargs.get("thread", 4)  # need psutil to read cpu core count

        # Threshold of data written to disk
        self.__flush_size = kwargs.get("flush", (1024 ** 2) * 100)

        # Download queue
        self.__queue = []
        # Cache pool
        self.cache = {}
        # Thread tasks pool
        self.__thread = ["" for _ in range(self.__thread_num)]
        # Tasks status pool
        self.status = {}

        # Start manager daemon thread
        self.__manager_thread = threading.Thread(target=self.__manager, name="Piuka Thread Manager")
        self.__manager_thread.daemon = True
        self.__manager_thread.start()

    def add(self, urls, dests, timeout=None):
        """
            Add download task

            :urls* -> str | list | tuple: URL(s) of the resource.
            :dests* -> str | list | tuple: File's destination(s), need to correspond to the URL(s) in turn.
            :timeout -> int: Time limit for a single request, in seconds.
        """
        assert type(urls) in (list, tuple, str)
        assert type(dests) in (list, tuple, str)
        if type(urls) in (list, tuple) and type(dests) in (list, tuple):
            assert len(urls) == len(dests)
        if timeout is None:
            timeout = self.__timeout
        assert type(timeout) == int

        # Construct a traversable object
        if type(urls) == str and type(dests) == str:
            urls = (urls,)
            dests = (dests,)

        queue = []
        for i in range(len(urls)):
            task_id = "{}-{}".format(time.time(), random.randint(1, 1000))
            queue.append((urls[i], dests[i], task_id))
            self.__queue.extend(queue)

        return queue

    def __manager(self):
        while True:
            for i in range(self.__thread_num):
                if not self.__thread[i] and self.__queue:
                    config = []
                    config.extend(self.__queue.pop(0))
                    config.append(i)
                    config = tuple(config)
                    self.__thread[i] = threading.Thread(target=self.__worker, args=config,
                                                        name="Piuka Worker Thread #{}".format(i + 1))
                    self.__thread[i].start()
            time.sleep(0.1)

    def __worker(self, url, dest, task_id, i):
        if dest != "::memory::":
            with open(dest, "wb") as file:
                error_count = 0
                while True:
                    try:
                        if error_count >= 10:
                            self.status[task_id] = (False, e)
                            self.__thread[i] = ""
                            return
                        else:
                            file_header = requests.head(url, headers=self.__headers, timeout=self.__timeout)
                    except Exception as e:
                        error_count += 1
                    else:
                        if file_header.status_code // 100 == 2:
                            break
                        else:
                            e = "http failed"
                            error_count += 1

                # Calc slice size
                length = int(file_header.headers["content-length"])
                slice_count = length // self.__flush_size
                if not slice_count:
                    slice_count = 1
                for j in range(0, length, length // slice_count):
                    header = self.__headers
                    next_size = j + (length // slice_count) - 1
                    if next_size > length:
                        next_size = length
                    header["range"] = "bytes={}-{}".format(j, next_size)
                    error_count = 0
                    while True:
                        try:
                            if error_count >= 10:
                                self.status[task_id] = (False, e)
                                self.__thread[i] = ""
                                return
                            else:
                                file_object = requests.get(url, headers=header, timeout=self.__timeout)
                        except Exception as e:
                            error_count += 1
                        else:
                            if file_object.status_code // 100 == 2:
                                break
                            else:
                                e = "http failed"
                                error_count += 1
                    file.write(file_object.content)
            self.status[task_id] = (True, None)
            self.__thread[i] = ""
        elif dest == "::memory::":
            error_count = 0
            while True:
                try:
                    if error_count >= 10:
                        self.status[task_id] = (False, e)
                        self.__thread[i] = ""
                        return
                    else:
                        file_object = requests.get(url, headers=self.__headers, timeout=self.__timeout)
                except Exception as e:
                    error_count += 1
                else:
                    if file_object.status_code // 100 == 2:
                        break
                    else:
                        e = "http failed"
                        error_count += 1
            self.cache[url] = file_object.content
            self.status[task_id] = (True, None)
            self.__thread[i] = ""