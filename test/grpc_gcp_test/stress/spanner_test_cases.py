from google.spanner.v1 import spanner_pb2

_DATABASE = 'projects/grpc-gcp/instances/sample/databases/benchmark'
_TEST_SQL = 'select id from storage'
_TEST_COLUMN_DATA = 'payload'
_TIMEOUT = 60 * 60 * 24

def test_execute_sql(stub):
    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))
    stub.ExecuteSql(
        spanner_pb2.ExecuteSqlRequest(session=session.name,
                                      sql=_TEST_SQL))
    stub.DeleteSession(
        spanner_pb2.DeleteSessionRequest(name=session.name))


def test_execute_sql_async(stub):
    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))
    response_future = stub.ExecuteSql.future(
        spanner_pb2.ExecuteSqlRequest(session=session.name,
                                      sql=_TEST_SQL))
    response_future.result()
    stub.DeleteSession(
        spanner_pb2.DeleteSessionRequest(name=session.name))


def test_execute_streaming_sql(stub):
    session = stub.CreateSession(
        spanner_pb2.CreateSessionRequest(database=_DATABASE))
    rendezvous = stub.ExecuteStreamingSql(
        spanner_pb2.ExecuteSqlRequest(
            session=session.name,
            sql=_TEST_SQL))
    for _ in rendezvous:
        pass
    stub.DeleteSession(
        spanner_pb2.DeleteSessionRequest(name=session.name))


def test_list_sessions(stub):
    stub.ListSessions(
        spanner_pb2.ListSessionsRequest(database=_DATABASE))

def test_list_sessions_async(stub):
    resp_future = stub.ListSessions.future(
        spanner_pb2.ListSessionsRequest(database=_DATABASE),
        _TIMEOUT)
    resp_future.result()


TEST_CASES = {
    'execute_sql': test_execute_sql,
    'execute_streaming_sql':test_execute_streaming_sql,
    'execute_sql_async': test_execute_sql_async,
    'list_sessions_async': test_list_sessions_async,
    'list_sessions': test_list_sessions,
}
