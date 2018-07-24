# Copyright 2018 gRPC-GCP authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import threading
import time
import timeit
import unittest

import google.protobuf.struct_pb2
import google.protobuf.text_format
import grpc
import grpc_gcp
import grpc_gcp._channel
import pkg_resources
from google.auth.transport.grpc import AuthMetadataPlugin
from google.auth.transport.requests import Request
from google.spanner.v1 import (keys_pb2, mutation_pb2, spanner_pb2,
                               spanner_pb2_grpc, transaction_pb2)

_TARGET = 'spanner.googleapis.com:443'
_DATABASE = 'projects/grpc-gcp/instances/sample/databases/benchmark'
_TABLE = 'large_table'
_STORAGE_ID_PAYLOAD = 'payload'
_OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
_TEST_CASE = 'execute_sql'
_NUM_OF_RPC = 100
_NUM_OF_THREAD = 10
_MAX_SIZE_PER_COLUMN = 4096000
_PAYLOAD_BYTES = 4096000
_GRPC_GCP = False
_TIMEOUT = 60 * 60 * 24
_NUM_WARM_UP_CALLS = 10


def _process_global_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--gcp', help='load gRPC-GCP extension', action='store_true')
    parser.add_argument(
        '--num_of_thread',
        type=int,
        help='num of max threads used during the benchmark')
    parser.add_argument(
        '--num_of_rpc', type=int, help='num of RPCs sent in each thread')
    parser.add_argument(
        '--payload_bytes', type=int, help='num of bytes of the payload')
    parser.add_argument(
        '--test_case', type=str, help='name of the call for benchmarking')
    args = parser.parse_args()
    if args.gcp:
        global _GRPC_GCP
        _GRPC_GCP = True
    if args.num_of_thread:
        global _NUM_OF_THREAD
        _NUM_OF_THREAD = args.num_of_thread
    if args.num_of_rpc:
        global _NUM_OF_RPC
        _NUM_OF_RPC = args.num_of_rpc
    if args.payload_bytes:
        global _PAYLOAD_BYTES
        _PAYLOAD_BYTES = args.payload_bytes
    if args.test_case:
        global _TEST_CASE
        _TEST_CASE = args.test_case


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

    print('\nUsing gRPC-GCP extension: {}'.format(_is_gcp_channel(channel)))

    return channel


def _is_gcp_channel(channel):
    return isinstance(channel, grpc_gcp._channel.Channel)


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


def _create_stub(channel):
    stub = spanner_pb2_grpc.SpannerStub(channel)
    return stub


def prepare_test_data():
    channel = _create_channel()
    stub = _create_stub(channel)

    print('Start adding payload to test table.')
    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))
    stub.Commit(
        spanner_pb2.CommitRequest(
            session=session.name,
            single_use_transaction=transaction_pb2.TransactionOptions(
                read_write=transaction_pb2.TransactionOptions.ReadWrite()),
            mutations=[
                mutation_pb2.Mutation(
                    delete=mutation_pb2.Mutation.Delete(
                        table=_TABLE, key_set=keys_pb2.KeySet(all=True)))
            ]))
    
    # because of max data size, we need to seperate into different rows
    column_bytes = min(_PAYLOAD_BYTES, _MAX_SIZE_PER_COLUMN)
    rows = (_PAYLOAD_BYTES - 1) / column_bytes + 1
    for i in range(rows):
        stub.Commit(
            spanner_pb2.CommitRequest(
                session=session.name,
                single_use_transaction=transaction_pb2.TransactionOptions(
                    read_write=transaction_pb2.TransactionOptions.ReadWrite()),
                mutations=[
                    mutation_pb2.Mutation(
                        insert_or_update=mutation_pb2.Mutation.Write(
                            table=_TABLE,
                            columns=['id', 'data'],
                            values=[
                                google.protobuf.struct_pb2.ListValue(
                                    values=[
                                        google.protobuf.struct_pb2.Value(
                                            string_value='payload{}'.format(i)),
                                        google.protobuf.struct_pb2.Value(
                                            string_value='x' * column_bytes)
                                    ])
                            ]))
                ]))
    print('Successfully add payload to table.')
    stub.DeleteSession(
        spanner_pb2.DeleteSessionRequest(name=session.name))


def _run_test(channel, func):
    # for num_of_thread in range(1, _NUM_OF_THREAD + 1):
    result = []
    threads = []
    if _NUM_OF_THREAD > 1:
        for tid in range(_NUM_OF_THREAD):
            thread = threading.Thread(
                target=func, name='tid_{}'.format(tid), args=(result,))
            threads.append(thread)
    start = timeit.default_timer()
    if _NUM_OF_THREAD > 1:
        for t in threads:
            t.start()
    else:
        func(result)
    for t in threads:
        t.join()

    while (True):
        # wait for all responses.
        if (len(result) >= _NUM_OF_RPC * _NUM_OF_THREAD):
            break

    result = sorted(result)
    print(('Threads, '
            'Channels, '
            'Avg(ms), '
            'Min(ms), '
            'Mean(ms), '
            'p90(ms), '
            'p99(ms), '
            'p100(ms), '
            'QPS'))
    print('{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}'.format(
        _NUM_OF_THREAD,
        len(channel._channel_refs) if _is_gcp_channel(channel) else 1,
        sum(result) / len(result) * 1000,
        result[0] * 1000,
        result[int(len(result) / 2)] * 1000,
        result[int(len(result) * 0.9)] * 1000,
        result[int(len(result) * 0.99)] * 1000,
        result[len(result) - 1] * 1000,
        _NUM_OF_RPC * _NUM_OF_THREAD /
        (timeit.default_timer() - start)))


def _handle_response(result, start_time, resp):
    resp.result()
    curr_thread_name = threading.currentThread().getName()
    print('{}: single rpc call finished!'.format(curr_thread_name))
    dur = timeit.default_timer() - start_time
    result.append(dur)


def test_execute_sql():
    channel = _create_channel()
    stub = _create_stub(channel)

    # warm up
    for _ in range(_NUM_WARM_UP_CALLS):
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        stub.ExecuteSql(
            spanner_pb2.ExecuteSqlRequest(
                session=session.name,
                sql='select data from storage'))
        stub.DeleteSession(
            spanner_pb2.DeleteSessionRequest(name=session.name))

    def execute_sql(result):
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        for _ in range(_NUM_OF_RPC):
            start = timeit.default_timer()
            stub.ExecuteSql(
                spanner_pb2.ExecuteSqlRequest(
                    session=session.name,
                    sql='select data from storage'))
            dur = timeit.default_timer() - start
            result.append(dur)
        stub.DeleteSession(
            spanner_pb2.DeleteSessionRequest(name=session.name))

    print('Executing blocking unary-unary call.')
    _run_test(channel, execute_sql)


def test_execute_sql_async():
    channel = _create_channel()
    stub = _create_stub(channel)

    # warm up
    for _ in range(_NUM_WARM_UP_CALLS):
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        resp_future = stub.ExecuteSql.future(
            spanner_pb2.ExecuteSqlRequest(
                session=session.name,
                sql='select data from large_table'))
        resp_future.result()
        stub.DeleteSession(
            spanner_pb2.DeleteSessionRequest(name=session.name))

    def execute_sql_async(result):
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        for _ in range(_NUM_OF_RPC):
            start = timeit.default_timer()
            resp_future = stub.ExecuteSql.future(
                spanner_pb2.ExecuteSqlRequest(
                    session=session.name,
                    sql='select data from large_table'),
                _TIMEOUT
                )
            resp_future.add_done_callback(
                lambda resp: _handle_response(result, start, resp))
        stub.DeleteSession(
            spanner_pb2.DeleteSessionRequest(name=session.name))

    print('Executing async unary-unary call.')
    _run_test(channel, execute_sql_async)


def test_execute_streaming_sql():
    channel = _create_channel()
    stub = _create_stub(channel)
    # _prepare_test_data(stub)

    # warm up
    print('Begin warm up calls.')
    for _ in range(_NUM_WARM_UP_CALLS):
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        rendezvous = stub.ExecuteStreamingSql(
            spanner_pb2.ExecuteSqlRequest(
                session=session.name,
                sql='select * from {}'.format(_TABLE)))
        for _ in rendezvous:
            pass
        stub.DeleteSession(
            spanner_pb2.DeleteSessionRequest(name=session.name))
    print('Warm up finished.')

    def execute_streaming_sql(result):
        curr_thread_name = threading.currentThread().getName()
        
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        for _ in range(_NUM_OF_RPC):
            start = timeit.default_timer()
            print('{}: start execute streaming sql at {}.'.format(curr_thread_name, start))
            rendezvous = stub.ExecuteStreamingSql(
                spanner_pb2.ExecuteSqlRequest(
                    session=session.name,
                    sql='select * from {}'.format(_TABLE)))
            rendezvous.add_done_callback(
                lambda resp: _handle_response(result, start, resp))
            for _ in rendezvous:
                pass
        stub.DeleteSession(
            spanner_pb2.DeleteSessionRequest(name=session.name))

    print('Executing unary-streaming call.')
    _run_test(channel, execute_streaming_sql)


class PrintLatencyCallback():
    def __init__(self, id, start_time):
        self.id = id
        self.start_time = start_time

    def __call__(self, resp):
        dur = (timeit.default_timer() - self.start_time) * 1000
        print('Finished {}th async call with {} ms...'.format(self.id, dur))


def test_unary_stream_concurrent_streams():
    channel = _create_channel()
    stub = _create_stub(channel)

    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))
    
    futures = []
    def execute_streaming_sql():
        
        rendezvous = stub.ExecuteStreamingSql(
            spanner_pb2.ExecuteSqlRequest(
                session=session.name,
                sql='select * from {}'.format(_TABLE)))
        futures.append(rendezvous)        

    for i in range(_NUM_OF_RPC):
        start = timeit.default_timer()
        execute_streaming_sql()
        print('{} --> started execute_streaming_sql with {} ms'.format(i+1, (time.time() - start) * 1000))
    
    list_sessions_start = time.time()
    stub.ListSessions(
        spanner_pb2.ListSessionsRequest(database=_DATABASE))
    list_sessions_dur = (time.time() - list_sessions_start) * 1000
    print('Finished list_sessions with {} ms'.format(list_sessions_dur))
    
    for future in futures:
        for _ in future:
            pass
        print('Finished one streaming response')

    stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


def test_unary_unary_concurrent_streams():
    channel = _create_channel()
    stub = _create_stub(channel)

    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))

    # warm up
    for i in range(_NUM_WARM_UP_CALLS):
        stub.ListSessions(
            spanner_pb2.ListSessionsRequest(database=_DATABASE))
    
    print('Done warming up.')
    
    futures = []

    for i in range(_NUM_OF_RPC):
        future = stub.ListSessions.future(
            spanner_pb2.ListSessionsRequest(database=_DATABASE))
        print('{} --> list_sessions async started'.format(i + 1))
        futures.append(future)

    print('--------------starting another call-------------')
    list_sessions_start = time.time()
    stub.ListSessions(
        spanner_pb2.ListSessionsRequest(database=_DATABASE))
    list_sessions_dur = (time.time() - list_sessions_start) * 1000
    print('--> list_sessions took {} ms'.format(list_sessions_dur))

    print('Waiting for result...')
    for future in futures:
        future.result()
    print('DONE.')

    stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


TEST_FUNCTIONS = {
    'prepare_test_data': prepare_test_data,
    'execute_sql': test_execute_sql,
    'execute_sql_async': test_execute_sql_async,
    'execute_streaming_sql': test_execute_streaming_sql,
    'unary_stream_concurrent_streams': test_unary_stream_concurrent_streams,
    'unary_unary_concurrent_streams':test_unary_unary_concurrent_streams,
}

if __name__ == "__main__":
    _process_global_arguments()
    test_function = TEST_FUNCTIONS[_TEST_CASE]
    test_function()
