from google.cloud import monitoring_v3

_PROJECT_ID = 'grpc-gcp'
_INSTANCE_ID = 'test-instance'
_INSTANCE_ZONE = 'us-central1-c'
_METRIC_TYPE = 'custom.googleapis.com/stress_test'
_RESOURCE_TYPE = 'gce_instance'

class StackdriverUtil:
    def __init__(self):
        self._client = monitoring_v3.MetricServiceClient()
        self._project_path = self._client.project_path(_PROJECT_ID)
        
    
    def add_timeseries(self, api, test_case, timestamp, duration_ms):
        series = monitoring_v3.types.TimeSeries()
        series.metric.type = '{}/{}/{}'.format(_METRIC_TYPE, api, test_case)
        series.resource.type = _RESOURCE_TYPE
        series.resource.labels['instance_id'] = _INSTANCE_ID
        series.resource.labels['zone'] = _INSTANCE_ZONE

        point = series.points.add()
        point.value.double_value = duration_ms
        point.interval.end_time.seconds = int(timestamp)
        point.interval.end_time.nanos = int(
            (timestamp - point.interval.end_time.seconds) * 10**9
        )

        self._client.create_time_series(self._project_path, [series])

        