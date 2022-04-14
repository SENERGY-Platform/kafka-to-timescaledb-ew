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

from ._util import *
import unittest
import json
import ew_lib
import ew
import psycopg2


with open("tests/resources/data.json") as file:
    data: list = json.load(file)

with open("tests/resources/filters.json") as file:
    filters: list = json.load(file)

with open("tests/resources/points_batch_results_l2.json") as file:
    gen_points_batch_results_l2: list = json.load(file)

with open("tests/resources/points_batch_results_l3.json") as file:
    gen_points_batch_results_l3: list = json.load(file)


class TestInfluxDBWorker(unittest.TestCase):
    def _init_export_worker(self, msg_error=False):
        mock_kafka_consumer_filter = MockKafkaConsumer(data=filters, sources=False)
        filter_client = ew_lib.FilterClient(
            kafka_consumer=mock_kafka_consumer_filter,
            filter_topic="filter",
            validator=ew.validate_filter,
            logger=logger
        )
        mock_kafka_consumer_data = MockKafkaConsumer(data=data, msg_error=msg_error, sources=False)
        data_client = ew_lib.DataClient(
            kafka_consumer=mock_kafka_consumer_data,
            filter_client=filter_client,
            kafka_msg_err_ignore=[3],
            logger=logger
        )
        export_worker = ew.ExportWorker(
            db_conn=psycopg2.connect(f"postgres://postgres:password@localhost:5432/test"),
            data_client=data_client,
            filter_client=filter_client
        )
        table_manager = ew.TableManager(
            db_conn=psycopg2.connect(f"postgres://postgres:password@localhost:5432/test"),
            filter_client=filter_client
        )
        filter_client.set_on_put(table_manager.create_table)
        filter_client.set_on_delete(table_manager.drop_table)
        table_manager.start()
        filter_client.start()
        while not mock_kafka_consumer_filter.empty():
            time.sleep(0.1)
        time.sleep(1)
        filter_client.stop()
        table_manager.stop()
        table_manager.join()
        filter_client.join()
        return export_worker, data_client, mock_kafka_consumer_data

    def test_gen_rows_batch(self):
        export_worker, data_client, mock_kafka_consumer = self._init_export_worker()
        while not mock_kafka_consumer.empty():
            exports_batch, _ = data_client.get_exports_batch(timeout=0.1, limit=10, data_ignore_missing_keys=True)
            if exports_batch:
                # export_worker._gen_rows_batch(exports_batch)
                export_worker._write_rows(rows_batch=export_worker._gen_rows_batch(exports_batch=exports_batch))

    # def _test_gen_points_batch(self, limit, results):
    #     export_worker, data_client, mock_kafka_consumer, _ = self._init_export_worker()
    #     count = 0
    #     while not mock_kafka_consumer.empty():
    #         exports_batch, _ = data_client.get_exports_batch(timeout=5, limit=limit)
    #         if exports_batch:
    #             self.assertIn(str(export_worker._gen_points_batch(exports_batch=exports_batch)), results)
    #             count += 1
    #     self.assertEqual(count, len(results))
    #
    # def test_gen_points_batch_l2(self):
    #     self._test_gen_points_batch(limit=2, results=gen_points_batch_results_l2)
    #
    # def test_gen_points_batch_l3(self):
    #     self._test_gen_points_batch(limit=3, results=gen_points_batch_results_l3)
    #
    # def test_write_points_batch(self):
    #     export_worker, data_client, mock_kafka_consumer, influxdb_client = self._init_export_worker()
    #     try:
    #         influxdb_client.ping()
    #     except Exception:
    #         self.skipTest("influxdb not reachable")
    #     while not mock_kafka_consumer.empty():
    #         exports_batch, _ = data_client.get_exports_batch(timeout=5, limit=2)
    #         if exports_batch:
    #             export_worker._write_points_batch(points_batch=export_worker._gen_points_batch(exports_batch=exports_batch))
    #     influxdb_client.close()
    #
    # def _test_run(self, filter_sync_err=False, msg_error=False):
    #     export_worker, _, _, _ = self._init_export_worker(msg_error=msg_error)
    #     export_worker.set_filter_sync(err=filter_sync_err)
    #     export_worker.run()
    #     self.assertFalse(export_worker.is_alive())
    #
    # def test_run_message_exception(self):
    #     self._test_run(msg_error=True)
    #
    # def test_run_filter_sync_err(self):
    #     self._test_run(filter_sync_err=True)


if __name__ == '__main__':
    unittest.main()
