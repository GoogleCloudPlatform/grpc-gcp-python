"""Tests for grpc_gcp_prober.spanner_probes."""

import unittest
from grpc_gcp_prober import spanner_probes
import mock
from google.protobuf import empty_pb2
from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2
from google.spanner.v1 import result_set_pb2
from google.spanner.v1 import spanner_pb2
from google.spanner.v1 import transaction_pb2


class SpannerProbesTest(unittest.TestCase):

  @mock.patch('google.spanner.v1.spanner_pb2_grpc.SpannerStub')
  def test_session_management(self, mock_stub_class):
    mock_stub = mock_stub_class.return_value
    test_session = spanner_pb2.Session(name='test_session')
    mock_stub.CreateSession.return_value = test_session
    mock_stub.GetSession.return_value = test_session
    mock_stub.ListSessions.return_value = spanner_pb2.ListSessionsResponse(
        sessions=[test_session])
    test_metrics = {}
    spanner_probes._session_management(mock_stub, test_metrics)
    mock_stub.CreateSession.assert_called_once()
    mock_stub.GetSession.assert_called_once()
    mock_stub.ListSessions.assert_called_once()
    mock_stub.DeleteSession.assert_called_once()
    self.assertGreater(len(test_metrics), 0)

  @mock.patch('google.spanner.v1.spanner_pb2_grpc.SpannerStub')
  def test_execute_sql(self, mock_stub_class):
    mock_stub = mock_stub_class.return_value
    test_session = spanner_pb2.Session(name='test_session')
    mock_stub.CreateSession.return_value = test_session
    mock_stub.ExecuteSql.return_value = result_set_pb2.ResultSet(rows=[
        struct_pb2.ListValue(
            values=[struct_pb2.Value(string_value='test_username')])
    ])
    mock_stub.ExecuteStreamingSql.return_value = iter([
        result_set_pb2.PartialResultSet(
            values=[struct_pb2.Value(string_value='test_username')])
    ])
    test_metrics = {}
    spanner_probes._execute_sql(mock_stub, test_metrics)
    mock_stub.CreateSession.assert_called_once()
    mock_stub.ExecuteSql.assert_called_once()
    mock_stub.ExecuteStreamingSql.assert_called_once()
    mock_stub.DeleteSession.assert_called_once()
    self.assertGreater(len(test_metrics), 0)

  @mock.patch('google.spanner.v1.spanner_pb2_grpc.SpannerStub')
  def test_read(self, mock_stub_class):
    mock_stub = mock_stub_class.return_value
    test_session = spanner_pb2.Session(name='test_session')
    mock_stub.CreateSession.return_value = test_session
    mock_stub.Read.return_value = result_set_pb2.ResultSet(rows=[
        struct_pb2.ListValue(
            values=[struct_pb2.Value(string_value='test_username')])
    ])
    mock_stub.StreamingRead.return_value = iter([
        result_set_pb2.PartialResultSet(
            values=[struct_pb2.Value(string_value='test_username')])
    ])
    test_metrics = {}
    spanner_probes._read(mock_stub, test_metrics)
    mock_stub.CreateSession.assert_called_once()
    mock_stub.Read.assert_called_once()
    mock_stub.StreamingRead.assert_called_once()
    mock_stub.DeleteSession.assert_called_once()
    self.assertGreater(len(test_metrics), 0)

  @mock.patch('google.spanner.v1.spanner_pb2_grpc.SpannerStub')
  def test_transaction(self, mock_stub_class):
    mock_stub = mock_stub_class.return_value
    test_session = spanner_pb2.Session(name='test_session')
    mock_stub.CreateSession.return_value = test_session
    mock_stub.BeginTransaction.return_value = \
        transaction_pb2.Transaction(id='1')
    mock_stub.Commit.return_value = spanner_pb2.CommitResponse(
        commit_timestamp=timestamp_pb2.Timestamp(seconds=1))
    mock_stub.RollbackRequest.return_value = empty_pb2.Empty()
    test_metrics = {}
    spanner_probes._transaction(mock_stub, test_metrics)
    mock_stub.CreateSession.assert_called_once()
    mock_stub.BeginTransaction.assert_called()
    mock_stub.Commit.assert_called_once()
    mock_stub.Rollback.assert_called_once()
    mock_stub.DeleteSession.assert_called_once()
    self.assertGreater(len(test_metrics), 0)

  @mock.patch('google.spanner.v1.spanner_pb2_grpc.SpannerStub')
  def test_partition(self, mock_stub_class):
    mock_stub = mock_stub_class.return_value
    test_session = spanner_pb2.Session(name='test_session')
    mock_stub.CreateSession.return_value = test_session
    test_partition_response = spanner_pb2.PartitionResponse(
        partitions=[spanner_pb2.Partition(partition_token='1')])
    mock_stub.PartitionQuery.return_value = test_partition_response
    mock_stub.PartitionRead.return_value = test_partition_response
    test_metrics = {}
    spanner_probes._partition(mock_stub, test_metrics)
    mock_stub.CreateSession.assert_called_once()
    mock_stub.PartitionQuery.assert_called_once()
    mock_stub.PartitionRead.assert_called_once()
    mock_stub.DeleteSession.assert_called_once()
    self.assertGreater(len(test_metrics), 0)


if __name__ == '__main__':
  unittest.main()
