"""Source code for the Firestore probes the cloudprober will execute.

Each method implements Firestore grpc client calls to it's grpc backend service.
The latency for each client call will be output to stackdriver as metrics. Note
that the metric output needs to be in a format of "key value" string.
e.g. "read_latency_ms 100"
"""

import time
from google.cloud.firestore_v1beta1.proto import firestore_pb2

_PARENT_RESOURCE = 'projects/grpc-prober-testing/databases/(default)/documents'


def _documents(stub, metrics):
  """Probes to test ListDocuments grpc call from Firestore stub.

  Args:
    stub: An object of FirestoreStub.
    metrics: A dict of metrics.
  """
  start = time.time()
  list_document_request = firestore_pb2.ListDocumentsRequest(
      parent=_PARENT_RESOURCE)
  stub.ListDocuments(list_document_request)
  latency = (time.time() - start) * 1000
  metrics['list_documents_latency_ms'] = latency


PROBE_FUNCTIONS = {
    'documents': _documents,
}
