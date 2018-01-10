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
from google.spanner.v1 import mutation_pb2
from google.spanner.v1 import keys_pb2
from google.spanner.v1 import spanner_pb2_grpc
from google.spanner.v1 import spanner_pb2
from google.spanner.v1 import transaction_pb2
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
import google.protobuf.struct_pb2
import google.protobuf.text_format
import grpc
import grpc_gcp
import grpc_gcp._channel
import grpc_gcp.grpc_gcp_pb2
import pkg_resources
import unittest
import timeit
import threading

_TARGET = 'spanner.googleapis.com:443'
_DATABASE = 'projects/grpc-gcp/instances/sample/databases/benchmark'
_TABLE = 'storage'
_STORAGE_ID_PAYLOAD = 'payload'
_OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
_NUM_OF_RPC = 100
_NUM_OF_THREAD = 10
_PAYLOAD_BYTES = 4096000
_GRPC_GCP = False


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


class SpannerTest(unittest.TestCase):
    def prepare_test_data(self):
        session = self.stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.stub.Commit(
            spanner_pb2.CommitRequest(
                session=session.name,
                single_use_transaction=transaction_pb2.TransactionOptions(
                    read_write=transaction_pb2.TransactionOptions.ReadWrite()),
                mutations=[
                    mutation_pb2.Mutation(
                        delete=mutation_pb2.Mutation.Delete(
                            table=_TABLE, key_set=keys_pb2.KeySet(all=True)))
                ]))
        self.stub.Commit(
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
                                            string_value=_STORAGE_ID_PAYLOAD),
                                        google.protobuf.struct_pb2.Value(
                                            string_value='x' * _PAYLOAD_BYTES)
                                    ])
                            ]))
                ]))
        self.stub.DeleteSession(
            spanner_pb2.DeleteSessionRequest(name=session.name))

    def warm_up(self):
        self.prepare_test_data()
        for i in range(_NUM_OF_THREAD):
            session = self.stub.CreateSession(
                spanner_pb2.CreateSessionRequest(database=_DATABASE))
            _ = self.stub.ExecuteSql(
                spanner_pb2.ExecuteSqlRequest(
                    session=session.name,
                    sql='select data from storage where id = \'%s\'' %
                    _STORAGE_ID_PAYLOAD))
            self.stub.DeleteSession(
                spanner_pb2.DeleteSessionRequest(name=session.name))

    def setUp(self):
        http_request = Request()
        credentials, _ = google.auth.default([_OAUTH_SCOPE], http_request)
        if _GRPC_GCP:
            config = grpc_gcp.grpc_gcp_pb2.ApiConfig()
            google.protobuf.text_format.Merge(
                pkg_resources.resource_string(__name__, 'spanner.grpc.config'),
                config)
            self.channel = secure_authorized_channel(
                credentials,
                http_request,
                _TARGET,
                options=[(grpc_gcp.GRPC_GCP_CHANNEL_ARG_API_CONFIG, config)])
        else:
            self.channel = secure_authorized_channel(credentials, http_request,
                                                     _TARGET)
        print('Using gRPC-GCP extension: {}'.format(
            isinstance(self.channel, grpc_gcp._channel.Channel)))
        self.stub = spanner_pb2_grpc.SpannerStub(self.channel)
        self.result = []
        self.warm_up()

    def is_gcp(self):
        return isinstance(self.channel, grpc_gcp._channel.Channel)

    def get_label(self):
        label = 'gRPC'
        if self.is_gcp():
            label += '-GCP'
        return label

    def test_execute_sql(self):
        def execute_sql():
            session = self.stub.CreateSession(
                spanner_pb2.CreateSessionRequest(database=_DATABASE))
            for i in range(_NUM_OF_RPC):
                start = timeit.default_timer()
                _ = self.stub.ExecuteSql(
                    spanner_pb2.ExecuteSqlRequest(
                        session=session.name,
                        sql='select data from storage where id = \'%s\'' %
                        _STORAGE_ID_PAYLOAD))
                dur = timeit.default_timer() - start
                self.result.append(dur)
            self.stub.DeleteSession(
                spanner_pb2.DeleteSessionRequest(name=session.name))

        print(('Threads, '
               'Channels, '
               'Avg(ms), '
               'Min(ms), '
               'Mean(ms), '
               'p90(ms), '
               'p99(ms), '
               'p100(ms), '
               'QPS'))
        for num_of_thread in range(1, _NUM_OF_THREAD + 1):
            self.result = []
            threads = []
            if num_of_thread > 1:
                for i in range(num_of_thread):
                    thread = threading.Thread(target=execute_sql)
                    threads.append(thread)
            start = timeit.default_timer()
            if num_of_thread > 1:
                for t in threads:
                    t.start()
            else:
                execute_sql()
            for t in threads:
                t.join()

            self.result = sorted(self.result)
            print('{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}'.format(
                num_of_thread,
                len(self.channel._channel_refs) if self.is_gcp() else 1,
                sum(self.result) / len(self.result) * 1000,
                self.result[0] * 1000,
                self.result[int(len(self.result) / 2)] * 1000,
                self.result[int(len(self.result) * 0.9)] * 1000,
                self.result[int(len(self.result) * 0.99)] * 1000,
                self.result[len(self.result) - 1] * 1000,
                _NUM_OF_RPC * num_of_thread /
                (timeit.default_timer() - start)))


if __name__ == "__main__":
    _process_global_arguments()
    print(unittest.TextTestRunner().run(unittest.makeSuite(SpannerTest)))
