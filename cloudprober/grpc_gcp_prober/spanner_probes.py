"""Source code for the Spanner probes the cloudprober will execute.

Each method implements Spanner grpc client calls to it's grpc backend service.
The latency for each client call will be output to stackdriver as metrics using
stackdriver_util.
"""

import time
from google.cloud.spanner_v1.proto import keys_pb2
from google.cloud.spanner_v1.proto import spanner_pb2
from google.cloud.spanner_v1.proto import transaction_pb2

_DATABASE = 'projects/grpc-prober-testing/instances/test-instance/databases/test-db'
_CLOUD_API_NAME = 'Spanner'
_TEST_USERNAME = 'test_username'


def _session_management(stub, metrics):
  """Probes to test session related grpc call from Spanner stub.

  Includes tests against CreateSession, GetSession, ListSessions, and
  DeleteSession of Spanner stub.

  Args:
    stub: An object of SpannerStub.
    metrics: A list of metrics.

  Raises:
    TypeError: An error occurred when result type is not as expected.
    ValueError: An error occurred when session name is not as expected.
  """
  session = None
  try:
    # Create session
    start = time.time()
    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))
    latency = (time.time() - start) * 1000
    metrics['create_session_latency_ms'] = latency

    if not isinstance(session, spanner_pb2.Session):
      raise TypeError(
          'response is of type %s, not spanner_pb2.Session!' % type(session))

    # Get session
    start = time.time()
    response = stub.GetSession(spanner_pb2.GetSessionRequest(name=session.name))
    latency = (time.time() - start) * 1000
    metrics['get_session_latency_ms'] = latency

    if not isinstance(response, spanner_pb2.Session):
      raise TypeError(
          'response is of type %s, not spanner_pb2.Session!' % type(response))
    if response.name != session.name:
      raise ValueError('incorrect session name %s' % response.name)

    # List sessions
    start = time.time()
    response = stub.ListSessions(
        spanner_pb2.ListSessionsRequest(database=_DATABASE))
    latency = (time.time() - start) * 1000
    metrics['list_sessions_latency_ms'] = latency

    session_list = response.sessions

    if session.name not in (s.name for s in session_list):
      raise ValueError(
          'session name %s is not in the result session list!' % session.name)

  finally:
    if session is not None:
      start = time.time()
      stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))
      latency = (time.time() - start) * 1000
      metrics['delete_session_latency_ms'] = latency


def _execute_sql(stub, metrics):
  """Probes to test ExecuteSql and ExecuteStreamingSql call from Spanner stub.

  Args:
    stub: An object of SpannerStub.
    metrics: A list of metrics.

  Raises:
    ValueError: An error occurred when sql result is not as expected.
  """
  session = None
  try:
    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))

    # Probing ExecuteSql call
    start = time.time()
    result_set = stub.ExecuteSql(
        spanner_pb2.ExecuteSqlRequest(
            session=session.name, sql='select * FROM users'))
    latency = (time.time() - start) * 1000
    metrics['execute_sql_latency_ms'] = latency

    if result_set is None:
      raise ValueError('result_set is None')
    if len(result_set.rows) != 1:
      raise ValueError('incorrect result_set rows %d' % len(result_set.rows))
    if result_set.rows[0].values[0].string_value != _TEST_USERNAME:
      raise ValueError(
          'incorrect sql result %s' % result_set.rows[0].values[0].string_value)

    # Probing ExecuteStreamingSql call
    partial_result_set = stub.ExecuteStreamingSql(
        spanner_pb2.ExecuteSqlRequest(
            session=session.name, sql='select * FROM users'))

    if partial_result_set is None:
      raise ValueError('streaming_result_set is None')

    start = time.time()
    first_result = partial_result_set.next()
    latency = (time.time() - start) * 1000
    metrics['execute_streaming_sql_latency_ms'] = latency

    if first_result.values[0].string_value != _TEST_USERNAME:
      raise ValueError('incorrect streaming sql first result %s' %
                       first_result.values[0].string_value)

  finally:
    if session is not None:
      stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


def _read(stub, metrics):
  """Probe to test Read and StreamingRead grpc call from Spanner stub.

  Args:
    stub: An object of SpannerStub.
    metrics: A list of metrics.

  Raises:
    ValueError: An error occurred when read result is not as expected.
  """
  session = None
  try:
    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))

    # Probing Read call
    start = time.time()
    result_set = stub.Read(
        spanner_pb2.ReadRequest(
            session=session.name,
            table='users',
            columns=['username', 'firstname', 'lastname'],
            key_set=keys_pb2.KeySet(all=True)))
    latency = (time.time() - start) * 1000
    metrics['read_latency_ms'] = latency

    if result_set is None:
      raise ValueError('result_set is None')
    if len(result_set.rows) != 1:
      raise ValueError('incorrect result_set rows %d' % len(result_set.rows))
    if result_set.rows[0].values[0].string_value != _TEST_USERNAME:
      raise ValueError(
          'incorrect sql result %s' % result_set.rows[0].values[0].string_value)

    # Probing StreamingRead call
    partial_result_set = stub.StreamingRead(
        spanner_pb2.ReadRequest(
            session=session.name,
            table='users',
            columns=['username', 'firstname', 'lastname'],
            key_set=keys_pb2.KeySet(all=True)))

    if partial_result_set is None:
      raise ValueError('streaming_result_set is None')

    start = time.time()
    first_result = partial_result_set.next()
    latency = (time.time() - start) * 1000
    metrics['streaming_read_latency_ms'] = latency

    if first_result.values[0].string_value != _TEST_USERNAME:
      raise ValueError('incorrect streaming sql first result %s' %
                       first_result.values[0].string_value)

  finally:
    if session is not None:
      stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


def _transaction(stub, metrics):
  """Probe to test BeginTransaction, Commit and Rollback grpc from Spanner stub.

  Args:
    stub: An object of SpannerStub.
    metrics: A list of metrics.
  """
  session = None
  try:
    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))

    txn_options = transaction_pb2.TransactionOptions(
        read_write=transaction_pb2.TransactionOptions.ReadWrite())
    txn_request = spanner_pb2.BeginTransactionRequest(
        session=session.name,
        options=txn_options,
    )

    # Probing BeginTransaction call
    start = time.time()
    txn = stub.BeginTransaction(txn_request)
    latency = (time.time() - start) * 1000
    metrics['begin_transaction_latency_ms'] = latency

    # Probing Commit call
    commit_request = spanner_pb2.CommitRequest(
        session=session.name, transaction_id=txn.id)
    start = time.time()
    stub.Commit(commit_request)
    latency = (time.time() - start) * 1000
    metrics['commit_latency_ms'] = latency

    # Probing Rollback call
    txn = stub.BeginTransaction(txn_request)
    rollback_request = spanner_pb2.RollbackRequest(
        session=session.name, transaction_id=txn.id)
    start = time.time()
    stub.Rollback(rollback_request)
    latency = (time.time() - start) * 1000
    metrics['rollback_latency_ms'] = latency

  finally:
    if session is not None:
      stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


def _partition(stub, metrics):
  """Probe to test PartitionQuery and PartitionRead grpc call from Spanner stub.

  Args:
    stub: An object of SpannerStub.
    metrics: A list of metrics.
  """
  session = None
  try:
    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))
    txn_options = transaction_pb2.TransactionOptions(
        read_only=transaction_pb2.TransactionOptions.ReadOnly())
    txn_selector = transaction_pb2.TransactionSelector(begin=txn_options)

    # Probing PartitionQuery call
    ptn_query_request = spanner_pb2.PartitionQueryRequest(
        session=session.name,
        sql='select * FROM users',
        transaction=txn_selector,
    )
    start = time.time()
    stub.PartitionQuery(ptn_query_request)
    latency = (time.time() - start) * 1000
    metrics['partition_query_latency_ms'] = latency

    # Probing PartitionRead call
    ptn_read_request = spanner_pb2.PartitionReadRequest(
        session=session.name,
        table='users',
        transaction=txn_selector,
        key_set=keys_pb2.KeySet(all=True),
        columns=['username', 'firstname', 'lastname'])
    start = time.time()
    stub.PartitionRead(ptn_read_request)
    latency = (time.time() - start) * 1000
    metrics['partition_read_latency_ms'] = latency

  finally:
    if session is not None:
      stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


PROBE_FUNCTIONS = {
    'session_management': _session_management,
    'execute_sql': _execute_sql,
    'read': _read,
    'transaction': _transaction,
    'partition': _partition,
}
