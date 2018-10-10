"""Tests for grpc_gcp_prober.firestore_probes."""

import unittest
from grpc_gcp_prober import firestore_probes
import mock


class FirestoreProbesTest(unittest.TestCase):

  @mock.patch('google.firestore.v1beta1.firestore_pb2_grpc.FirestoreStub')
  def test_probe_documents(self, mock_stub_class):
    mock_stub = mock_stub_class.return_value
    test_metrics = {}
    firestore_probes._documents(mock_stub, test_metrics)
    mock_stub.ListDocuments.assert_called_once()
    self.assertGreater(len(test_metrics), 0)


if __name__ == '__main__':
  unittest.main()
