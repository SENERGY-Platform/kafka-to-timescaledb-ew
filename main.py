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

import util
import ew
import ew_lib
import cncr_wdg
import confluent_kafka
import psycopg2
import signal


if __name__ == '__main__':
    util.print_init(name="kafka-to-timescaledb-ew", git_info_file="git_commit")
    config = util.Config(prefix="conf")
    util.init_logger(config.logger_level)
    util.logger.debug(f"export worker config: {config}")
    db_conn_ew = psycopg2.connect(
        host=config.timescaledb.host,
        port=config.timescaledb.port,
        database=config.timescaledb.database,
        user=config.timescaledb.username,
        password=config.timescaledb.password
    )
    db_conn_tm = psycopg2.connect(
        host=config.timescaledb.host,
        port=config.timescaledb.port,
        database=config.timescaledb.database,
        user=config.timescaledb.username,
        password=config.timescaledb.password
    )
    kafka_filter_consumer_config = {
        "metadata.broker.list": config.kafka.metadata_broker_list,
        "group.id": f"{config.kafka_filter_consumer_group_id}_{config.kafka.consumer_group_id_postfix}",
        "auto.offset.reset": "earliest",
    }
    util.logger.debug(f"kafka filter consumer config: {kafka_filter_consumer_config}")
    kafka_filter_consumer = confluent_kafka.Consumer(kafka_filter_consumer_config, logger=util.logger)
    filter_client = ew_lib.FilterClient(
        kafka_consumer=kafka_filter_consumer,
        filter_topic=config.kafka_filter_client.filter_topic,
        poll_timeout=config.kafka_filter_client.poll_timeout,
        time_format=config.kafka_filter_client.time_format,
        utc=config.kafka_filter_client.utc,
        validator=ew.validate_filter,
        logger=util.logger
    )
    kafka_data_consumer_config = {
        "metadata.broker.list": config.kafka.metadata_broker_list,
        "group.id": f"{config.kafka_data_consumer.group_id}_{config.kafka.consumer_group_id_postfix}",
        "auto.offset.reset": config.kafka_data_consumer.auto_offset_reset,
        "partition.assignment.strategy": config.kafka_data_consumer.partition_assignment_strategy,
        "enable.auto.offset.store": False
    }
    util.logger.debug(f"kafka data consumer config: {kafka_data_consumer_config}")
    kafka_data_consumer = confluent_kafka.Consumer(kafka_data_consumer_config, logger=util.logger)
    data_client = ew_lib.DataClient(
        kafka_consumer=kafka_data_consumer,
        filter_client=filter_client,
        subscribe_interval=config.kafka_data_client.subscribe_interval,
        handle_offsets=True,
        kafka_msg_err_ignore=[int(e) for e in config.kafka_data_client.kafka_msg_err_ignore.split(",")] if isinstance(config.kafka_data_client.kafka_msg_err_ignore, str) and config.kafka_data_client.kafka_msg_err_ignore else [config.kafka_data_client.kafka_msg_err_ignore],
        logger=util.logger
    )
    export_worker = ew.ExportWorker(
        db_conn=db_conn_ew,
        data_client=data_client,
        filter_client=filter_client,
        get_data_timeout=config.get_data_timeout,
        get_data_limit=config.get_data_limit,
        page_size=config.page_size
    )
    table_manager = ew.TableManager(
        db_conn=db_conn_tm,
        filter_client=filter_client,
        distributed_hypertables=config.timescaledb.distributed_hypertables,
        hypertable_replication_factor=config.timescaledb.hypertable_replication_factor,
        timeout=config.table_manager.timeout,
        retries=config.table_manager.retries,
        retry_delay=config.table_manager.retry_delay,
    )
    filter_client.set_on_sync(callable=export_worker.set_filter_sync, sync_delay=config.kafka_filter_client.sync_delay)
    filter_client.set_on_put(callable=table_manager.create_table)
    filter_client.set_on_delete(callable=table_manager.drop_table)
    watchdog = cncr_wdg.Watchdog(
        monitor_callables=[export_worker.is_alive, filter_client.is_alive, data_client.is_alive],
        shutdown_callables=[export_worker.stop, data_client.stop, filter_client.stop],
        join_callables=[data_client.join, filter_client.join, db_conn_ew.close, db_conn_tm.close, kafka_data_consumer.close, kafka_filter_consumer.close],
        shutdown_signals=[signal.SIGTERM, signal.SIGINT, signal.SIGABRT],
        monitor_delay=config.watchdog.monitor_delay,
        logger=util.logger
    )
    watchdog.start(delay=config.watchdog.start_delay)
    filter_client.start()
    data_client.start()
    export_worker.run()
    watchdog.join()
