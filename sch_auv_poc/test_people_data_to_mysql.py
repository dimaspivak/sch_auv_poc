"""Tests for the "Tour de France Data to Elasticsearch" pipeline."""

import logging
import time
import urllib

import requests
import sqlalchemy
from sqlalchemy import Column, Integer, MetaData, String, Table
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


def test_complete(sch, database):
    """A test of the complete functioning of the pipeline."""
    pipeline = sch.pipelines.get(name='People Data to MySQL')

    SAMPLE_DATA = [dict(name='justin', age=39), dict(name='jc', age=43), dict(name='chris', age=48),
                   dict(name='joey', age=43), dict(name='lance', age=40)]

    table_name = get_random_string()
    table = Table(table_name, MetaData(),
                  Column('id', Integer, primary_key=True), Column('name', String(100)), Column('age', Integer))
    try:
        logger.info('Creating table (%s) in database ...', table_name)
        table.create(database.engine)

        runtime_parameters = dict(JDBC_CONNECTION_STRING=database.jdbc_connection_string,
                                  JDBC_USERNAME=database.username,
                                  JDBC_PASSWORD=database.password,
                                  TABLE_NAME=table_name)

        with sch.run_test_job(pipeline, runtime_parameters, data_collector_labels=sch.data_collector_labels) as job:
            http_listening_port = pipeline.stages.get(label='HTTP Server 1').http_listening_port
            parse_result = urllib.parse.urlparse(job.data_collectors[0].url)
            http_server_endpoint = f'{parse_result.scheme}://{parse_result.hostname}:{http_listening_port}'
            for entry in SAMPLE_DATA:
                requests.post(http_server_endpoint, json=entry, headers={'X-SDC-APPLICATION-ID': 'abc123'}, verify=False)

            with database.engine.begin() as connection:
                mysql_data = [dict(row) for row in connection.execute(table.select()).fetchall()]

                # mysql_data includes auto-incrementing row ID, so remove it before asserting.
                assert SAMPLE_DATA == [dict(name=row['name'], age=row['age']) for row in mysql_data]

    finally:
        logger.info('Dropping table %s ...', table_name)
        table.drop(database.engine)


