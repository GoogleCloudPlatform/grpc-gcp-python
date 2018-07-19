import random
import threading
import time
import traceback
import grpc
import grpc_gcp
import google.protobuf.struct_pb2
import google.protobuf.text_format
import pkg_resources

from google.auth.transport.grpc import AuthMetadataPlugin
from google.auth.transport.requests import Request
from google.spanner.v1 import (keys_pb2, mutation_pb2,
                               spanner_pb2_grpc, transaction_pb2)
from six.moves import queue
from grpc_gcp_test.stress import spanner_test_cases

_TARGET = 'spanner.googleapis.com'
_OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
_WEIGHTED_TEST_CASES = 'execute_sql:100'
_NUM_CHANNELS_PER_TARGET = 1
_NUM_STUBS_PER_CHANNEL = 1
_TIMEOUT_SECS = -1 # Default no timeout
_GRPC_GCP = False


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
                # start_time = time.time()
                test_case(self._stub)
                # end_time = time.time()
                print('-------------SUCCESS---------------')
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

    if _GRPC_GCP:
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
    weighted_test_cases = _parse_weighted_test_cases(_WEIGHTED_TEST_CASES)
    exception_queue = queue.Queue()
    stop_event = threading.Event()
    runners = []

    for _ in xrange(_NUM_CHANNELS_PER_TARGET):
        channel = _create_channel()
        for _ in xrange(_NUM_STUBS_PER_CHANNEL):
            stub = spanner_pb2_grpc.SpannerStub(channel)
            runner = TestRunner(stub, weighted_test_cases, exception_queue, stop_event)
            runners.append(runner)

    for runner in runners:
        runner.start()

    try:
        timeout = _TIMEOUT_SECS if _TIMEOUT_SECS >= 0 else None
        raise exception_queue.get(block=True, timeout=timeout)
    except queue.Empty:
        pass
    finally:
        stop_event.set()
        for runner in runners:
            runner.join()
        runner = None


if __name__ == '__main__':
    run_test()



