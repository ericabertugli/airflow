# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
from unittest import mock

from airflow.models import DAG, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.log.cloudwatch_task_handler import CloudwatchTaskHandler
from airflow.utils.state import State
from airflow.utils.timezone import datetime

try:
    import boto3
    import moto
    from moto import mock_logs
except ImportError:
    mock_logs = None

@unittest.skipIf(mock_logs is None,
                 "Skipping test because moto.mock_logs is not available")
@mock_logs
class TestCloudwatchTaskHandler(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.log_group = 'my_log_group'
        self.log_stream = 'my_log_stream'
        self.region_name = 'us-west-2'
        self.formatter = ''
        self.cloudwatch_task_handler = CloudwatchTaskHandler(
            self.log_group,
            self.region_name,
            self.formatter
        )

        date = datetime(2016, 1, 1)
        self.dag = DAG('dag_for_testing_cloudwatch_task_handler', start_date=date)
        task = DummyOperator(task_id='task_for_testing_cloudwatch_task_handler', dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=date)
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.addCleanup(self.dag.clear)

        self.conn = boto3.client('logs')
        moto.core.moto_api_backend.reset()
        self.conn.create_log_group(logGroupName=self.log_group)
        self.conn.create_log_stream(logGroupName=self.log_group, logStreamName=self.log_stream)

    def test_read(self):
        loggorups = self.conn.describe_log_groups(
            logGroupNamePrefix='my'
        )
        print("-------aaaaaaaaaaaaaa----------")
        print(loggorups)
        self.conn.put_log_events(
            logGroupName=self.log_group,
            logStreamName=self.log_stream,
            logEvents=[
                {
                    'timestamp': 100,
                    'message': 'my_log_message'
                },
            ]
        )
        # logs.create_log_stream(
        #    logGroupName=log_group_name,
        #    logStreamName=log_stream_name,
        #)
        response = self.conn.describe_log_groups(
            logGroupNamePrefix='log'
        )
        print(response)
        self.assertEqual(
            self.cloudwatch_task_handler.read(self.ti),
            'my_log_message'
        )

    # def test_read_when_log_missing(self):
    #     log, metadata = self.s3_task_handler.read(self.ti)
    #
    #     self.assertEqual(1, len(log))
    #     self.assertEqual(len(log), len(metadata))
    #     self.assertIn('*** Log file does not exist:', log[0])
    #     self.assertEqual({'end_of_log': True}, metadata[0])
    #
    # def test_read_raises_return_error(self):
    #     handler = self.s3_task_handler
    #     url = 's3://nonexistentbucket/foo'
    #     with mock.patch.object(handler.log, 'error') as mock_error:
    #         result = handler.s3_read(url, return_error=True)
    #         msg = 'Could not read logs from %s' % url
    #         self.assertEqual(result, msg)
    #         mock_error.assert_called_once_with(msg, exc_info=True)
    #
    # def test_write(self):
    #     with mock.patch.object(self.s3_task_handler.log, 'error') as mock_error:
    #         self.s3_task_handler.s3_write('text', self.remote_log_location)
    #         # We shouldn't expect any error logs in the default working case.
    #         mock_error.assert_not_called()
    #     body = boto3.resource('s3').Object('bucket', self.remote_log_key).get()['Body'].read()
    #
    #     self.assertEqual(body, b'text')
    #
    # def test_write_existing(self):
    #     self.conn.put_object(Bucket='bucket', Key=self.remote_log_key, Body=b'previous ')
    #     self.s3_task_handler.s3_write('text', self.remote_log_location)
    #     body = boto3.resource('s3').Object('bucket', self.remote_log_key).get()['Body'].read()
    #
    #     self.assertEqual(body, b'previous \ntext')
    #
    # def test_write_raises(self):
    #     handler = self.s3_task_handler
    #     url = 's3://nonexistentbucket/foo'
    #     with mock.patch.object(handler.log, 'error') as mock_error:
    #         handler.s3_write('text', url)
    #         self.assertEqual
    #         mock_error.assert_called_once_with(
    #             'Could not write logs to %s', url, exc_info=True)
    #
    # def test_close(self):
    #     self.s3_task_handler.set_context(self.ti)
    #     self.assertTrue(self.s3_task_handler.upload_on_close)
    #
    #     self.s3_task_handler.close()
    #     # Should not raise
    #     boto3.resource('s3').Object('bucket', self.remote_log_key).get()
    #
    # def test_close_no_upload(self):
    #     self.ti.raw = True
    #     self.s3_task_handler.set_context(self.ti)
    #     self.assertFalse(self.s3_task_handler.upload_on_close)
    #     self.s3_task_handler.close()
    #
    #     with self.assertRaises(self.conn.exceptions.NoSuchKey):
    #         boto3.resource('s3').Object('bucket', self.remote_log_key).get()
