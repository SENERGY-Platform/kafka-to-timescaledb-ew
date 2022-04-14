"""
   Copyright 2022 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import confluent_kafka
import json
import queue
import typing
import time
import logging


logger = logging.getLogger("ew")
# logger.setLevel(logging.DEBUG)
# logger.disabled = True


class MockKafkaError:
    def __init__(self, fatal=False, code=None):
        self.__fatal = fatal
        self.__code = code

    def fatal(self,):
        return self.__fatal

    def retriable(self):
        return not self.__fatal

    def str(self):
        return "error text"

    def code(self):
        return self.__code


class MockKafkaMessage:
    def __init__(self, value=None, topic=None, err_obj=None, partition=0, offset=None):
        self.__value = value
        self.__err_obj = err_obj
        self.__topic = topic
        self.__partition = partition
        self.__offset = offset
        self.__timestamp = (confluent_kafka.TIMESTAMP_LOG_APPEND_TIME, time.time())

    def error(self) -> MockKafkaError:
        return self.__err_obj

    def value(self):
        return self.__value

    def topic(self):
        return self.__topic

    def timestamp(self):
        return self.__timestamp

    def partition(self):
        return self.__partition

    def offset(self):
        return self.__offset


class MockKafkaConsumer(confluent_kafka.Consumer):
    def __init__(self, data: typing.Union[typing.Dict, typing.List], sources: bool = True, msg_error: bool = False, sub_topics=None):
        self.__sources = sources
        self.__queue = queue.Queue()
        self.__offsets = dict()
        self.__sub_topics = {item: False for item in sub_topics} if sub_topics else sub_topics
        err_objs = (
            MockKafkaError(code=1),
            MockKafkaError(fatal=True, code=2),
            MockKafkaError(code=3)
        )
        if self.__sources:
            for source in data:
                offset = 0
                for message in data[source]:
                    self.__queue.put(MockKafkaMessage(value=json.dumps(message), topic=source, offset=offset))
                    offset += 1
                if msg_error:
                    for err_obj in err_objs:
                        self.__queue.put(MockKafkaMessage(err_obj=err_obj, topic=source))
        else:
            offset = 0
            for message in data:
                self.__queue.put(MockKafkaMessage(value=json.dumps(message), offset=offset))
                offset += 1
            if msg_error:
                for err_obj in err_objs:
                    self.__queue.put(MockKafkaMessage(err_obj=err_obj))

    def subscribe(self, topics, on_assign=None, *args, **kwargs):
        if self.__sub_topics:
            for topic in topics:
                self.__sub_topics[topic] = True

    def poll(self, timeout=None):
        try:
            return self.__queue.get(timeout=timeout)
        except queue.Empty:
            pass

    def consume(self, num_messages=1, timeout=None, *args, **kwargs):
        msgs = list()
        while len(msgs) < num_messages:
            try:
                msgs.append(self.__queue.get(timeout=timeout))
            except queue.Empty:
                break
        return msgs

    def empty(self):
        return self.__queue.empty()

    def subscribed(self):
        return all(self.__sub_topics.values())

    def store_offsets(self, offsets):
        for tp in offsets:
            assert isinstance(tp, confluent_kafka.TopicPartition)
            key = f"{tp.topic}{tp.partition}"
            if key not in self.__offsets:
                assert tp.offset == 1
            else:
                assert tp.offset == self.__offsets[key] + 1
            self.__offsets[key] = tp.offset
