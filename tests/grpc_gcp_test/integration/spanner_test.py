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
"""Tests grpc_gcp channel behaviors for Spanner APIs.

Database schema:

    Column           | Type   | Nullable
    ------------------------------------
    id (Primary key) | STRING | No
    data             | BYTES  | Yes

Test data:

    id        | data
    -----------------------
    'payload' | <data blob>

"""

import threading
import unittest

import google.protobuf.text_format
import grpc
import grpc_gcp
import pkg_resources
from google.auth.transport.grpc import AuthMetadataPlugin
from google.auth.transport.requests import Request
from google.spanner.v1 import spanner_pb2, spanner_pb2_grpc

_TARGET = 'spanner.googleapis.com'
_DATABASE = 'projects/grpc-gcp/instances/sample/databases/benchmark'
_TEST_SQL = 'select id from storage'
_TEST_COLUMN_DATA = 'payload'
_OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
_DEFAULT_MAX_CHANNELS_PER_TARGET = 10


class _Callback(object):
    def __init__(self):
        self._condition = threading.Condition()
        self._first_connectivities = []
        self._second_connectivites = []

    def update_first(self, connectivity):
        with self._condition:
            self._first_connectivities.append(connectivity)
            self._condition.notify()

    def update_second(self, connectivity):
        with self._condition:
            self._second_connectivites.append(connectivity)
            self._condition.notify()

    def block_until_connectivities_satisfy(self, predicate, first=True):
        with self._condition:
            while True:
                connectivities = tuple(self._first_connectivities
                                       if first else self._second_connectivites)
                if predicate(connectivities):
                    return connectivities
                else:
                    self._condition.wait()


class SpannerTest(unittest.TestCase):
    def setUp(self):
        config = grpc_gcp.api_config_from_text_pb(
            pkg_resources.resource_string(__name__, 'spanner.grpc.config'))
        http_request = Request()
        credentials, _ = google.auth.default([_OAUTH_SCOPE], http_request)
        self.channel = self._create_secure_gcp_channel(
            credentials,
            http_request,
            _TARGET,
            options=[(grpc_gcp.API_CONFIG_CHANNEL_ARG, config)])
        self.assertIsInstance(self.channel, grpc_gcp._channel.Channel)
        self.assertEqual(self.channel._max_concurrent_streams_low_watermark, 1)
        self.assertEqual(self.channel._max_size, 10)
    
    def _create_secure_gcp_channel(
            self, credentials, request, target, ssl_credentials=None, **kwargs):
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

    def test_create_session_reuse_channel(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        for _ in range(_DEFAULT_MAX_CHANNELS_PER_TARGET * 2):
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
                session=session.name,
                sql=_TEST_SQL))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(result_set)
        self.assertEqual(1, len(result_set.rows))
        self.assertEqual(_TEST_COLUMN_DATA,
                         result_set.rows[0].values[0].string_value)
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
        result_set, _ = stub.ExecuteSql.with_call(
            spanner_pb2.ExecuteSqlRequest(
                session=session.name,
                sql=_TEST_SQL))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(result_set)
        self.assertEqual(1, len(result_set.rows))
        self.assertEqual(_TEST_COLUMN_DATA,
                         result_set.rows[0].values[0].string_value)
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
                session=session.name,
                sql=_TEST_SQL))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(1, self.channel._channel_refs[0]._active_stream_ref)
        result_set = rendezvous.result()
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(result_set)
        self.assertEqual(1, len(result_set.rows))
        self.assertEqual(_TEST_COLUMN_DATA,
                         result_set.rows[0].values[0].string_value)
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
                session=session.name,
                sql=_TEST_SQL))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(1, self.channel._channel_refs[0]._active_stream_ref)
        self.assertIsNotNone(rendezvous)
        for partial_result_set in rendezvous:
            self.assertEqual(_TEST_COLUMN_DATA,
                             partial_result_set.values[0].string_value)
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(1, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))
        self.assertEqual(1, len(self.channel._channel_refs))
        self.assertEqual(0, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)

    def test_concurrent_streams_watermark(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        watermark = 2
        self.channel._max_concurrent_streams_low_watermark = watermark
        self.assertEqual(self.channel._max_concurrent_streams_low_watermark, watermark)

        session_list = []
        rendezvous_list = []

        # When active streams have not reached the concurrent_streams_watermark,
        # gRPC calls should be reusing the same channel.
        for i in range(watermark):
            session = stub.CreateSession(
                spanner_pb2.CreateSessionRequest(database=_DATABASE))
            self.assertEqual(1, len(self.channel._channel_refs))
            self.assertEqual(i + 1, self.channel._channel_refs[0]._affinity_ref)
            self.assertEqual(i, self.channel._channel_refs[0]._active_stream_ref)
            self.assertIsNotNone(session)
            session_list.append(session)

            rendezvous = stub.ExecuteStreamingSql(
                spanner_pb2.ExecuteSqlRequest(
                    session=session.name,
                    sql=_TEST_SQL))
            self.assertEqual(1, len(self.channel._channel_refs))
            self.assertEqual(i + 1, self.channel._channel_refs[0]._affinity_ref)
            self.assertEqual(i + 1, self.channel._channel_refs[0]._active_stream_ref)
            rendezvous_list.append(rendezvous)

        # When active streams reach the concurrent_streams_watermark,
        # channel pool will create a new channel.
        another_session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.assertEqual(2, len(self.channel._channel_refs))
        self.assertEqual(2, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(2, self.channel._channel_refs[0]._active_stream_ref)
        self.assertEqual(1, self.channel._channel_refs[1]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[1]._active_stream_ref)
        self.assertIsNotNone(another_session)
        session_list.append(another_session)

        another_rendezvous = stub.ExecuteStreamingSql(
            spanner_pb2.ExecuteSqlRequest(
                session=another_session.name,
                sql=_TEST_SQL))
        self.assertEqual(2, len(self.channel._channel_refs))
        self.assertEqual(2, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(2, self.channel._channel_refs[0]._active_stream_ref)
        self.assertEqual(1, self.channel._channel_refs[1]._affinity_ref)
        self.assertEqual(1, self.channel._channel_refs[1]._active_stream_ref)
        rendezvous_list.append(another_rendezvous)

        # Iterate through the rendezous list to clean active streams.
        for rendezvous in rendezvous_list:
            for _ in rendezvous:
                continue

        # After cleaning, previously created channels will remain in the pool.
        self.assertEqual(2, len(self.channel._channel_refs))
        self.assertEqual(2, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertEqual(1, self.channel._channel_refs[1]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[1]._active_stream_ref)

        # Delete all sessions to clean affinity.
        for session in session_list:
            stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))

        self.assertEqual(2, len(self.channel._channel_refs))
        self.assertEqual(0, self.channel._channel_refs[0]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[0]._active_stream_ref)
        self.assertEqual(0, self.channel._channel_refs[1]._affinity_ref)
        self.assertEqual(0, self.channel._channel_refs[1]._active_stream_ref)

    def test_bound_unbind_with_invalid_affinity_key(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)

        with self.assertRaises(Exception) as context:
            stub.GetSession(
                spanner_pb2.GetSessionRequest(name='random_name'))
        self.assertEqual(grpc.StatusCode.INVALID_ARGUMENT,
                         context.exception.code())

        with self.assertRaises(Exception) as context:
            stub.DeleteSession(
                spanner_pb2.DeleteSessionRequest(name='random_name'))
        self.assertEqual(grpc.StatusCode.INVALID_ARGUMENT,
                         context.exception.code())

    def test_bound_after_unbind(self):
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.assertEqual(1, len(self.channel._channel_ref_by_affinity_key))
        stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))
        self.assertEqual(0, len(self.channel._channel_ref_by_affinity_key))
        with self.assertRaises(Exception) as context:
            stub.GetSession(
                spanner_pb2.GetSessionRequest(name=session.name))
        self.assertEqual(grpc.StatusCode.NOT_FOUND,
                         context.exception.code())

    def test_channel_connectivity(self):
        callback = _Callback()

        self.channel.subscribe(callback.update_first, try_to_connect=False)
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        connectivities = callback.block_until_connectivities_satisfy(
            lambda connectivities: grpc.ChannelConnectivity.READY in connectivities)
        self.assertEqual(3, len(connectivities))
        self.assertSequenceEqual((grpc.ChannelConnectivity.IDLE,
                                  grpc.ChannelConnectivity.CONNECTING,
                                  grpc.ChannelConnectivity.READY),
                                 connectivities)
        stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))

        self.channel.unsubscribe(callback.update_first)
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.assertEqual(3, len(connectivities))
        stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))

    def test_channel_connectivity_multiple_subchannels(self):
        callback = _Callback()

        self.channel.subscribe(callback.update_first, try_to_connect=False)
        stub = spanner_pb2_grpc.SpannerStub(self.channel)
        futures = []
        for _ in range (2):
            futures.append(stub.CreateSession.future(
                spanner_pb2.CreateSessionRequest(database=_DATABASE)))
        connectivities = callback.block_until_connectivities_satisfy(
            lambda connectivities: grpc.ChannelConnectivity.READY in connectivities)

        self.assertEqual(2, len(self.channel._channel_refs))
        self.assertSequenceEqual((grpc.ChannelConnectivity.IDLE,
                                  grpc.ChannelConnectivity.CONNECTING,
                                  grpc.ChannelConnectivity.READY),
                                 connectivities)
        for future in futures:
            stub.DeleteSession(
                spanner_pb2.DeleteSessionRequest(name=future.result().name))
        
    def test_channel_connectivity_invalid_target(self):
        config = config = grpc_gcp.api_config_from_text_pb(
            pkg_resources.resource_string(__name__, 'spanner.grpc.config'))
        http_request = Request()
        credentials, _ = google.auth.default([_OAUTH_SCOPE], http_request)
        invalid_channel = self._create_secure_gcp_channel(
            credentials,
            http_request,
            'localhost:1234',
            options=[(grpc_gcp.API_CONFIG_CHANNEL_ARG, config)])

        callback = _Callback()
        invalid_channel.subscribe(callback.update_first, try_to_connect=False)

        stub = spanner_pb2_grpc.SpannerStub(invalid_channel)
        with self.assertRaises(Exception) as context:
            stub.CreateSession(
                spanner_pb2.CreateSessionRequest(database=_DATABASE))
        self.assertEqual(grpc.StatusCode.UNAVAILABLE,
                         context.exception.code())
        first_connectivities = callback.block_until_connectivities_satisfy(
            lambda connectivities: len(connectivities) >= 3)
        self.assertEqual(grpc.ChannelConnectivity.IDLE, first_connectivities[0])
        self.assertIn(grpc.ChannelConnectivity.CONNECTING, first_connectivities)
        self.assertIn(grpc.ChannelConnectivity.TRANSIENT_FAILURE, first_connectivities)

        invalid_channel.subscribe(callback.update_second, try_to_connect=True)
        second_connectivities = callback.block_until_connectivities_satisfy(
            lambda connectivities: len(connectivities) >= 3, False)
        self.assertNotIn(grpc.ChannelConnectivity.IDLE, second_connectivities)
        self.assertIn(grpc.ChannelConnectivity.CONNECTING, second_connectivities)
        self.assertIn(grpc.ChannelConnectivity.TRANSIENT_FAILURE, second_connectivities)

        self.assertEqual(2, len(invalid_channel._subscribers))
        invalid_channel.unsubscribe(callback.update_first)
        invalid_channel.unsubscribe(callback.update_second)
        self.assertEqual(0, len(invalid_channel._subscribers))


if __name__ == "__main__":
    unittest.main()
