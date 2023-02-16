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

__all__ = ("Config", )

import sevm


class KafkaConfig(sevm.Config):
    metadata_broker_list = None
    id_postfix = None


class KafkaDataConsumerConfig(sevm.Config):
    group_id = None
    auto_offset_reset = "earliest"
    partition_assignment_strategy = "cooperative-sticky"


class KafkaDataClientConfig(sevm.Config):
    subscribe_interval = 5
    kafka_msg_err_ignore = 3


class KafkaFilterClientConfig(sevm.Config):
    filter_topic = None
    poll_timeout = 1.0
    sync_delay = 30
    time_format = None
    utc = True


class TimescaleDBConfig(sevm.Config):
    host = None
    port = None
    username = None
    password = None
    database = None
    distributed_hypertables = False
    hypertable_replication_factor = 2


class TableManagerConfig(sevm.Config):
    timeout = 1
    retries = 2
    retry_delay = 2


class WatchdogConfig(sevm.Config):
    monitor_delay = 2
    start_delay = 5


class Config(sevm.Config):
    logger_level = "warning"
    get_data_timeout = 5.0
    get_data_limit = 1000
    page_size = 100
    kafka = KafkaConfig
    kafka_data_client = KafkaDataClientConfig
    kafka_data_consumer = KafkaDataConsumerConfig
    kafka_filter_client = KafkaFilterClientConfig
    kafka_filter_consumer_group_id = None
    watchdog = WatchdogConfig
    timescaledb = TimescaleDBConfig
    table_manager = TableManagerConfig
