"""Tests for the "Tour de France Data to Elasticsearch" pipeline."""

import logging
import string
import time

import pytest
import sqlalchemy
from elasticsearch_dsl import Index
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


def test_complete(sch, database, elasticsearch):
    """A test of the complete functioning of the pipeline."""
    pipeline = sch.pipelines.get(name='Tour de France Data to Elasticsearch')

    SAMPLE_DATA = [dict(year=1903, rank=1, name='MAURICE GARIN', number=1, team='TDF 1903',
                        time='94h 33m 14s', hours=94, mins=33, secs=14),
                   dict(year=1903, rank=2, name='LUCIEN POTHIER', number=37, team='TDF 1903',
                        time='97h 32m 35s', hours=97, mins=32, secs=35),
                   dict(year=1903, rank=3, name='FERNAND AUGEREAU', number=39, team='TDF 1903',
                        time='99h 02m 38s', hours=99, mins=2, secs=38)]
    EXPECTED_RECORDS = [dict(year=1903, rank=1, firstName='Maurice', lastName='Garin', number=1, team='TDF 1903',
                             time='94h 33m 14s', hours=94, mins=33, secs=14),
                        dict(year=1903, rank=2, firstName='Lucien', lastName='Pothier', number=37, team='TDF 1903',
                             time='97h 32m 35s', hours=97, mins=32, secs=35),
                        dict(year=1903, rank=3, firstName='Fernand', lastName='Augereau', number=39, team='TDF 1903',
                             time='99h 02m 38s', hours=99, mins=2, secs=38)]

    table_name = get_random_string()
    index = get_random_string(string.ascii_lowercase)

    table = sqlalchemy.Table(table_name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('year', sqlalchemy.Integer),
                             sqlalchemy.Column('rank', sqlalchemy.Integer),
                             sqlalchemy.Column('name', sqlalchemy.String(100)),
                             sqlalchemy.Column('number', sqlalchemy.Integer),
                             sqlalchemy.Column('team', sqlalchemy.String(100)),
                             sqlalchemy.Column('time', sqlalchemy.String(100)),
                             sqlalchemy.Column('hours', sqlalchemy.Integer),
                             sqlalchemy.Column('mins', sqlalchemy.Integer),
                             sqlalchemy.Column('secs', sqlalchemy.Integer))
    try:
        logger.info('Creating table (%s) in database ...', table_name)
        table.create(database.engine)

        logger.info('Inserting sample data ...')
        connection = database.engine.connect()
        connection.execute(table.insert(), SAMPLE_DATA)

        runtime_parameters = dict(JDBC_CONNECTION_STRING=database.jdbc_connection_string,
                                  JDBC_USERNAME=database.username,
                                  JDBC_PASSWORD=database.password,
                                  ELASTICSEARCH_URI=f'{elasticsearch.hostname}:{elasticsearch.port}',
                                  ELASTICSEARCH_CREDENTIALS=f'{elasticsearch.username}:{elasticsearch.password}',
                                  ELASTICSEARCH_INDEX=index,
                                  TABLE_NAME_PATTERN=f'%{table_name}%')

        with sch.run_test_job(pipeline, runtime_parameters, data_collector_labels=['test']) as job:
            time.sleep(10)
            data_in_elasticsearch = [hit.to_dict() for hit in elasticsearch.search(index=index).sort('rank').execute()]
            assert EXPECTED_RECORDS == data_in_elasticsearch
    finally:
        index = Index(index, using=elasticsearch.client)
        if index.exists():
            logger.info('Deleting Elasticsearch index %s ...', index)
            index.delete()

        logger.info('Dropping table %s ...', table_name)
        table.drop(database.engine)
