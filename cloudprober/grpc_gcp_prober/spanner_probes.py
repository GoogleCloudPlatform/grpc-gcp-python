"""Source code for the Spanner probes the cloudprober will execute.

Each method implements Spanner grpc client calls to it's grpc backend service.
The latency for each client call will be output to stackdriver as metrics using
stackdriver_util.
"""

from tracer import initialize_tracer

from google.cloud.spanner_v1.proto import keys_pb2
from google.cloud.spanner_v1.proto import spanner_pb2
from google.cloud.spanner_v1.proto import transaction_pb2


_DATABASE = 'projects/grpc-prober-testing/instances/test-instance/databases/test-db'
_CLOUD_API_NAME = 'Spanner'
_TEST_USERNAME = 'test_username'


def _session_management(stub):
  """Probes to test session related grpc call from Spanner stub.

  Includes tests against CreateSession, GetSession, ListSessions, and
  DeleteSession of Spanner stub.

  Args:
    stub: An object of SpannerStub.

  Raises:
    TypeError: An error occurred when result type is not as expected.
    ValueError: An error occurred when session name is not as expected.
  """
  _session_management_tracer = initialize_tracer()
  with _session_management_tracer.span(name='_session_management'):
    session = None
    try:
      # Create session
      with _session_management_tracer.span(name='stub.CreateSession'):
        session = stub.CreateSession(spanner_pb2.CreateSessionRequest(database=_DATABASE))

      if not isinstance(session, spanner_pb2.Session):
        raise TypeError(
          'response is of type %s, not spanner_pb2.Session!' % type(session))

      # Get session
      with _session_management_tracer.span(name='stub.GetSession'):
        response = stub.GetSession(spanner_pb2.GetSessionRequest(name=session.name))

      if not isinstance(response, spanner_pb2.Session):
        raise TypeError(
          'response is of type %s, not spanner_pb2.Session!' % type(response))
      if response.name != session.name:
        raise ValueError('incorrect session name %s' % response.name)

      # List session
      with _session_management_tracer.span(name='stub.ListSessions'):
        response = stub.ListSessions(
            spanner_pb2.ListSessionsRequest(database=_DATABASE))

      session_list = response.sessions

      if session.name not in (s.name for s in session_list):
        raise ValueError(
          'session name %s is not in the result session list!' % session.name)

    finally:
      if session is not None:
        # Delete session
        with _session_management_tracer.span(name='stub.DeleteSession'):
          stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))



def _execute_sql(stub):
  """Probes to test ExecuteSql and ExecuteStreamingSql call from Spanner stub.

  Args:
    stub: An object of SpannerStub.

  Raises:
    ValueError: An error occurred when sql result is not as expected.
  """
  _execute_sql_tracer = initialize_tracer()
  with _execute_sql_tracer.span(name='_execute_sql'):
    session = None
    try:
      # Create session
      with _execute_sql_tracer.span(name='stub.CreateSession'):
        session = stub.CreateSession(
          spanner_pb2.CreateSessionRequest(database=_DATABASE))

      # Probing ExecuteSql call
      with _execute_sql_tracer.span(name='stub.ExecuteSql'):
        result_set = stub.ExecuteSql(
          spanner_pb2.ExecuteSqlRequest(
            session=session.name, sql='select * FROM users'))

      if result_set is None:
        raise ValueError('result_set is None')
      if len(result_set.rows) != 1:
        raise ValueError('incorrect result_set rows %d' % len(result_set.rows))
      if result_set.rows[0].values[0].string_value != _TEST_USERNAME:
        raise ValueError(
          'incorrect sql result %s' % result_set.rows[0].values[0].string_value)

      # Probing ExecuteStreamingSql call
      with _execute_sql_tracer.span(name='stub.ExecuteStreamingSql'):
        partial_result_set = stub.ExecuteStreamingSql(
          spanner_pb2.ExecuteSqlRequest(
            session=session.name, sql='select * FROM users'))

      if partial_result_set is None:
        raise ValueError('streaming_result_set is None')

      with _execute_sql_tracer.span(name='partial_result_set.next'):
        first_result = partial_result_set.next()

      if first_result.values[0].string_value != _TEST_USERNAME:
        raise ValueError('incorrect streaming sql first result %s' %
                         first_result.values[0].string_value)

    finally:
      if session is not None:
        with _execute_sql_tracer.span(name='stub.DeleteSession'):
          stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


def _read(stub):
  """Probe to test Read and StreamingRead grpc call from Spanner stub.

  Args:
    stub: An object of SpannerStub.

  Raises:
    ValueError: An error occurred when read result is not as expected.
  """
  _read_tracer = initialize_tracer()
  with _read_tracer.span(name='_read'):
    session = None
    try:
      # Create session
      with _read_tracer.span(name='stub.CreateSession'):
        session = stub.CreateSession(
          spanner_pb2.CreateSessionRequest(database=_DATABASE))

      # Probing Read call
      with _read_tracer.span(name='stub.Read'):
        result_set = stub.Read(
              spanner_pb2.ReadRequest(
                  session=session.name,
                  table='users',
                  columns=['username', 'firstname', 'lastname'],
                  key_set=keys_pb2.KeySet(all=True)))

      if result_set is None:
        raise ValueError('result_set is None')
      if len(result_set.rows) != 1:
        raise ValueError('incorrect result_set rows %d' % len(result_set.rows))
      if result_set.rows[0].values[0].string_value != _TEST_USERNAME:
        raise ValueError(
            'incorrect sql result %s' % result_set.rows[0].values[0].string_value)

      # Probing StreamingRead call
      with _read_tracer.span(name='stub.StreamingRead'):
        partial_result_set = stub.StreamingRead(
            spanner_pb2.ReadRequest(
                session=session.name,
                table='users',
                columns=['username', 'firstname', 'lastname'],
                key_set=keys_pb2.KeySet(all=True)))

      if partial_result_set is None:
        raise ValueError('streaming_result_set is None')

      with _read_tracer.span(name='partial_result_set.next'):
        first_result = partial_result_set.next()

      if first_result.values[0].string_value != _TEST_USERNAME:
        raise ValueError('incorrect streaming sql first result %s' %
                         first_result.values[0].string_value)

    finally:
      if session is not None:
        with _read_tracer.span(name='stub.DeleteSession'):
          stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


def _transaction(stub):
  """Probe to test BeginTransaction, Commit and Rollback grpc from Spanner stub.

  Args:
    stub: An object of SpannerStub.
  """
  _transaction_tracer = initialize_tracer()
  with _transaction_tracer.span(name='_transaction'):
    session = None
    try:
      with _transaction_tracer.span(name='stub.CreateSession'):
        session = stub.CreateSession(
            spanner_pb2.CreateSessionRequest(database=_DATABASE))

      txn_options = transaction_pb2.TransactionOptions(
          read_write=transaction_pb2.TransactionOptions.ReadWrite())
      txn_request = spanner_pb2.BeginTransactionRequest(
          session=session.name,
          options=txn_options,
      )

      # Probing BeginTransaction call
      with _transaction_tracer.span(name='stub.BeginTransaction'):
        txn = stub.BeginTransaction(txn_request)

      # Probing Commit call
      commit_request = spanner_pb2.CommitRequest(
          session=session.name, transaction_id=txn.id)
      with _transaction_tracer.span(name='stub.Commit'):
        stub.Commit(commit_request)

      # Probing Rollback call
      txn = stub.BeginTransaction(txn_request)
      rollback_request = spanner_pb2.RollbackRequest(
          session=session.name, transaction_id=txn.id)
      with _transaction_tracer.span(name='stub.Rollback'):
        stub.Rollback(rollback_request)

    finally:
      if session is not None:
        with _transaction_tracer.span(name='stub.DeleteSession'):
          stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


def _partition(stub):
  """Probe to test PartitionQuery and PartitionRead grpc call from Spanner stub.

  Args:
    stub: An object of SpannerStub.
  """
  _partition_tracer = initialize_tracer()
  with _partition_tracer.span(name='_partition'):
    session = None
    try:

      with _partition_tracer.span(name='stub.CreateSession'):
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
      with _partition_tracer.span(name='stub.PartitionQuery'):
        stub.PartitionQuery(ptn_query_request)

      # Probing PartitionRead call
      ptn_read_request = spanner_pb2.PartitionReadRequest(
        session=session.name,
        table='users',
        transaction=txn_selector,
        key_set=keys_pb2.KeySet(all=True),
        columns=['username', 'firstname', 'lastname'])
      with _partition_tracer.span(name='stub.PartitionRead'):
        stub.PartitionRead(ptn_read_request)

    finally:
      if session is not None:
        with _partition_tracer.span(name='stub.DeleteSession'):
          stub.DeleteSession(spanner_pb2.DeleteSessionRequest(name=session.name))


PROBE_FUNCTIONS = {
    'session_management': _session_management,
    'execute_sql': _execute_sql,
    'read': _read,
    'transaction': _transaction,
    'partition': _partition,
}
