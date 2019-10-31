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

import logging

import boto3
import watchtower

from jinja2 import Template

from airflow.utils.log.logging_mixin import LoggingMixin


class CloudwatchTaskHandler(logging.Handler, LoggingMixin):
    """
        CloudwatchTaskHandler is a python log handler that handles and reads
        task instance logs. It extends python logging.Handler and
        uploads to and reads from Amazon CloudWatch.
        :param log_group: CloudWatch Log Group where the logs are read/written
        :param logname_template: template for the log name
        :param region_name: AWS Region Name (example: us-west-2)
        :param formatter: formatter used to write the log
        """
    def __init__(self, log_group, logname_template, region_name=None, formatter=None, **kwargs):
        super(CloudwatchTaskHandler, self).__init__()
        self.handler = None
        self.log_group = log_group
        self.region_name = region_name
        self.formatter = formatter
        self.logname_template = logname_template
        self.filename_jinja_template = None
        self.kwargs = kwargs
        self.closed = False

        if "{{" in self.logname_template:  # jinja mode
            self.filename_jinja_template = Template(self.logname_template)

    def _render_filename(self, ti, try_number):
        if self.filename_jinja_template:
            jinja_context = ti.get_template_context()
            jinja_context['try_number'] = try_number
            return (
                self.filename_jinja_template.render(**jinja_context)
                .replace(':', '_')
            )

        return self.logname_template.format(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            execution_date=ti.execution_date.isoformat(),
            try_number=try_number,
        ).replace(':', '_')

    def set_context(self, ti):
        """
        Provide task_instance context to airflow task handler.
        :param ti: task instance object
        """
        kwargs = self.kwargs.copy()
        stream_name = kwargs.pop('stream_name', None)
        if stream_name is None:
            stream_name = self._render_filename(ti, ti.try_number)
        if 'boto3_session' not in kwargs and self.region_name is not None:
            kwargs['boto3_session'] = boto3.session.Session(
                region_name=self.region_name,
            )
        self.handler = watchtower.CloudWatchLogHandler(
            log_group=self.log_group,
            stream_name=stream_name,
            **kwargs
        )
        self.handler.setFormatter(self.formatter)

    def emit(self, record):
        if self.handler is not None:
            self.handler.emit(record)

    def flush(self):
        if self.handler is not None:
            self.handler.flush()

    def close(self):
        """
        Close and upload local log file to remote storage S3.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        if self.handler is not None:
            self.handler.close()
        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def read(self, task_instance, try_number=None):
        """
        Read logs of given task instance from cloudwatch.
        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
                           it returns all logs separated by try_number
        :return: a list of logs
        """
        if try_number is None:
            next_try = task_instance.next_try_number
            try_numbers = list(range(1, next_try))
        elif try_number < 1:
            logs = [
                'Error fetching the logs. Try number {try_number} is invalid.',
            ]
            return logs
        else:
            try_numbers = [try_number]

        logs = [''] * len(try_numbers)
        for i, try_num in enumerate(try_numbers):
            logs[i] += self._read(task_instance, try_num)

        return logs

    def _read(self, task_instance, try_number):
        stream_name = self._render_filename(task_instance, try_number)
        if self.handler is not None:
            client = self.handler.cwl_client
        else:
            client = boto3.client('logs', region_name=self.region_name)
        events = []
        try:
            response = client.get_log_events(
                logGroupName=self.log_group,
                logStreamName=stream_name,
            )
            events.extend(response['events'])
            next_token = response['nextForwardToken']
            while True:
                response = client.get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=stream_name,
                )
                if next_token == response['nextForwardToken']:
                    break
                events.extend(response['events'])
                next_token = response['nextForwardToken']
        except client.exceptions.ResourceNotFoundException:
            return 'Log stream {0} does not exist'.format(stream_name)
        return '\n'.join(event['message'] for event in events)
