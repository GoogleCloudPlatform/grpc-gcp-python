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
from google.spanner.v1 import spanner_pb2_grpc
from google.spanner.v1 import spanner_pb2
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
import google.protobuf.text_format
import grpc_gcp._channel
import grpc_gcp.grpc_gcp_pb2
import pkg_resources
import unittest

_TARGET = 'spanner.googleapis.com'
_DATABASE = 'projects/grpc-gcp/instances/sample/databases/benchmark'
_OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
_DEFAULT_MAX_CHANNELS_PER_TARGET = 10


class SpannerTest(unittest.TestCase):
    def setUp(self):
        config = grpc_gcp.grpc_gcp_pb2.ApiConfig()
        google.protobuf.text_format.Merge(
            pkg_resources.resource_string(__name__, 'spanner.grpc.config'),
            config)
        http_request = Request()
        credentials, _ = google.auth.default([_OAUTH_SCOPE], http_request)
        self.channel = secure_authorized_channel(
            credentials,
            http_request,
            _TARGET,
            options=[(grpc_gcp.GRPC_GCP_CHANNEL_ARG_API_CONFIG, config)])
        self.assertIsInstance(self.channel, grpc_gcp._channel.Channel)

    def test_create_session_reuse_channel(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        for i in range(_DEFAULT_MAX_CHANNELS_PER_TARGET):
            session = stub.CreateSession(
                spanner_pb2.CreateSessionRequest(database=_DATABASE))
            self.assertIsNotNone(session)
            self.assertEqual(1, len(self.channel._channel_refs))
            stub.DeleteSession(
                spanner_pb2.DeleteSessionRequest(name=session.name))
        for i in range(_DEFAULT_MAX_CHANNELS_PER_TARGET):
            session = stub.CreateSession(
                spanner_pb2.CreateSessionRequest(database=_DATABASE))
            self.assertIsNotNone(session)
            self.assertEqual(1, len(self.channel._channel_refs))
            stub.DeleteSession(
                spanner_pb2.DeleteSessionRequest(name=session.name))

    def test_create_session_new_channel(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        futures = []
        for i in range(_DEFAULT_MAX_CHANNELS_PER_TARGET):
            futures.append(
                stub.CreateSession.future(
                    spanner_pb2.CreateSessionRequest(database=_DATABASE)))
            self.assertEqual(i + 1, len(self.channel._channel_refs))
        for future in futures:
            stub.DeleteSession(
                spanner_pb2.DeleteSessionRequest(name=future.result().name))
        futures = []
        for i in range(_DEFAULT_MAX_CHANNELS_PER_TARGET):
            futures.append(
                stub.CreateSession.future(
                    spanner_pb2.CreateSessionRequest(database=_DATABASE)))
            self.assertEqual(_DEFAULT_MAX_CHANNELS_PER_TARGET,
                             len(self.channel._channel_refs))
        for future in futures:
            stub.DeleteSession(
                spanner_pb2.DeleteSessionRequest(name=future.result().name))

    def test_create_list_delete_session(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.assertIsNotNone(session)
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        sessions = stub.ListSessions(
            spanner_pb2.ListSessionsRequest(database=_DATABASE))
        self.assertIsNotNone(sessions.sessions)
        self.assertIn(session.name, (s.name for s in sessions.sessions))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(0, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        sessions = stub.ListSessions(
            spanner_pb2.ListSessionsRequest(database=_DATABASE))
        self.assertNotIn(session.name, (s.name for s in sessions.sessions))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(0, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)

    def test_execute_sql(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.assertIsNotNone(session)
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        result_set = stub.ExecuteSql(
            spanner_pb2.ExecuteSqlRequest(
                session=session.name, sql='select id from storage'))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(result_set)
        self.assertEqual(1, len(result_set.rows))
        self.assertEqual('payload', result_set.rows[0].values[0].string_value)
        stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(0, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)

    def test_execute_sql_with_call(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.assertIsNotNone(session)
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        result_set, rendezvous = stub.ExecuteSql.with_call(
            spanner_pb2.ExecuteSqlRequest(
                session=session.name, sql='select id from storage'))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(result_set)
        self.assertEqual(1, len(result_set.rows))
        self.assertEqual('payload', result_set.rows[0].values[0].string_value)
        stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(0, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)

    def test_execute_sql_future(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(session)
        rendezvous = stub.ExecuteSql.future(
            spanner_pb2.ExecuteSqlRequest(
                session=session.name, sql='select id from storage'))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(1, self.channel._channel_refs[0]._active_stream_ref)
        result_set = rendezvous.result()
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(result_set)
        self.assertEqual(1, len(result_set.rows))
        self.assertEqual('payload', result_set.rows[0].values[0].string_value)
        stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(0, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)

    def test_execute_streaming_sql(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(session)
        rendezvous = stub.ExecuteStreamingSql(
            spanner_pb2.ExecuteSqlRequest(
                session=session.name, sql='select id from storage'))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(1, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(rendezvous)
        for partial_result_set in rendezvous:
            self.assertEqual('payload',
                             partial_result_set.values[0].string_value)
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(0, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)


if __name__ == "__main__":
    unittest.main()
