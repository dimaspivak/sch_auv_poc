"""Tests for the "Trivial" pipeline."""

import json


def test_complete(sch):
    """A test of the complete functioning of the pipeline."""
    pipeline = sch.pipelines.get(name='Trivial')

    SAMPLE_DATA = dict(company='StreamSets', valuation='$1,000,000,000')
    runtime_parameters = dict(RAW_DATA=json.dumps(SAMPLE_DATA))

    with sch.run_test_job(pipeline, runtime_parameters, data_collector_labels=sch.data_collector_labels) as job:
        job_data_collector = job.data_collectors[0]
        data_collector = job_data_collector.instance
        pipeline = job_data_collector.pipeline

        snapshot = data_collector.capture_snapshot(pipeline).snapshot
        assert [record.field for record in snapshot[pipeline.origin_stage].output] == [SAMPLE_DATA]
