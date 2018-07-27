import random
import sys
import threading
import timeit
import traceback

import google.protobuf.struct_pb2
import google.protobuf.text_format
import grpc
import grpc_gcp
import pkg_resources
from absl import flags
from google.auth.transport.grpc import AuthMetadataPlugin
from google.auth.transport.requests import Request
from google.spanner.v1 import spanner_pb2_grpc
from grpc_gcp_test.stress import spanner_test_cases, stackdriver_util

from six.moves import queue

_TARGET = 'spanner.googleapis.com'
_OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'

FLAGS = flags.FLAGS
flags.DEFINE_string('weighted_cases', 'execute_sql:100',
                    'comma seperated list of testcase:weighting')
flags.DEFINE_string('api', 'spanner',
                    'name of cloud api for stress testing')
flags.DEFINE_integer('num_channels_per_target', 1,
                     'number of channels per target')
flags.DEFINE_integer('num_stubs_per_channel', 1,
                     'number of stubs to create per channel')
flags.DEFINE_integer('timeout_secs', -1,
                     'timeout in seconds for the stress test')
flags.DEFINE_boolean('gcp', False, 'load grpc gcp extension')


util = stackdriver_util.StackdriverUtil()

class TestRunner(threading.Thread):
    def __init__(self, stub, weighted_test_cases, exception_queue, stop_event):
        super(TestRunner, self).__init__()
        self._exception_queue = exception_queue
        self._stop_event = stop_event
        self._stub = stub
        self._test_cases_generator = _weighted_test_case_generator(weighted_test_cases)

    def run(self):
        while not self._stop_event.is_set():
            try:
                test_case = next(self._test_cases_generator)
                start_time = timeit.default_timer()
                test_case(self._stub)
                end_time = timeit.default_timer()
                duration_ms = (end_time - start_time) * 1000
                sys.stdout.write('.')
                sys.stdout.flush()
                util.add_timeseries(FLAGS.api, test_case.__name__, end_time, duration_ms)
            except Exception as e:
                traceback.print_exc()
                self._exception_queue.put(
                    Exception("An exception occured during test {}"
                              .format(test_case), e))


def _weighted_test_case_generator(weighted_cases):
    weight_sum = sum(weighted_cases.itervalues())

    while True:
        val = random.uniform(0, weight_sum)
        partial_sum = 0
        for case in weighted_cases:
            partial_sum += weighted_cases[case]
            if val <= partial_sum:
                yield case
                break


def _create_channel():
    http_request = Request()
    credentials, _ = google.auth.default([_OAUTH_SCOPE], http_request)

    if FLAGS.gcp:
        config = grpc_gcp.api_config_from_text_pb(
            pkg_resources.resource_string(__name__, 'spanner.grpc.config'))
        channel = _create_secure_gcp_channel(
            credentials,
            http_request,
            _TARGET,
            options=[(grpc_gcp.API_CONFIG_CHANNEL_ARG, config)])
    else:
        channel = _create_secure_gcp_channel(
            credentials, http_request, _TARGET)

    return channel


def _create_secure_gcp_channel(
        credentials, request, target, ssl_credentials=None, **kwargs):
    # This method is copied from
    # google.auth.transport.grpc.secure_authorized_channel but using
    # grpc_gcp.secure_channel to create the channel.
    metadata_plugin = AuthMetadataPlugin(credentials, request)
    google_auth_credentials = grpc.metadata_call_credentials(metadata_plugin)
    if ssl_credentials is None:
        ssl_credentials = grpc.ssl_channel_credentials()
    composite_credentials = grpc.composite_channel_credentials(
        ssl_credentials, google_auth_credentials)
    return grpc_gcp.secure_channel(target, composite_credentials, **kwargs)


def _parse_weighted_test_cases(test_case_args):
    weighted_test_cases = {}
    for test_case_arg in test_case_args.split(','):
        name, weight = test_case_arg.split(':', 1)
        test_case = spanner_test_cases.TEST_CASES[name]
        weighted_test_cases[test_case] = int(weight)
    return weighted_test_cases


def run_test():
    weighted_test_cases = _parse_weighted_test_cases(FLAGS.weighted_cases)
    exception_queue = queue.Queue()
    stop_event = threading.Event()
    runners = []

    for _ in xrange(FLAGS.num_channels_per_target):
        channel = _create_channel()
        for _ in xrange(FLAGS.num_stubs_per_channel):
            stub = spanner_pb2_grpc.SpannerStub(channel)
            runner = TestRunner(stub, weighted_test_cases, exception_queue, stop_event)
            runners.append(runner)

    for runner in runners:
        runner.start()

    try:
        timeout = FLAGS.timeout_secs if FLAGS.timeout_secs >= 0 else None
        raise exception_queue.get(block=True, timeout=timeout)
    except queue.Empty:
        pass
    finally:
        stop_event.set()
        for runner in runners:
            runner.join()
        runner = None


if __name__ == '__main__':
    FLAGS(sys.argv)
    run_test()
