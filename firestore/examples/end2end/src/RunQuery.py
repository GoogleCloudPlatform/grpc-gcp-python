#! /usr/bin/python
import sys
import os
import json
import grpc
import time
import subprocess

from google.oauth2 import service_account
import google.oauth2.credentials
import google.auth.transport.requests
import google.auth.transport.grpc

from google.firestore.v1beta1 import firestore_pb2
from google.firestore.v1beta1 import firestore_pb2_grpc
from google.firestore.v1beta1 import document_pb2
from google.firestore.v1beta1 import document_pb2_grpc
from google.firestore.v1beta1 import common_pb2
from google.firestore.v1beta1 import common_pb2_grpc
from google.firestore.v1beta1 import write_pb2
from google.firestore.v1beta1 import write_pb2_grpc
from google.firestore.v1beta1 import query_pb2
from google.firestore.v1beta1 import query_pb2_grpc

from google.protobuf import empty_pb2
from google.protobuf import timestamp_pb2
from google.protobuf import wrappers_pb2

   

def main():

   fl = os.path.dirname(os.path.abspath(__file__))
   fn = os.path.join(fl, 'grpc.json')

   with open(fn) as grpc_file:
          
            item = json.load(grpc_file)

            creds = item["grpc"]["RunQuery"]["credentials"]

            credentials = service_account.Credentials.from_service_account_file("{}".format(creds))
            scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
            http_request = google.auth.transport.requests.Request()
            channel = google.auth.transport.grpc.secure_authorized_channel(scoped_credentials, http_request, 'firestore.googleapis.com:443')

            stub = firestore_pb2_grpc.FirestoreStub(channel)

            
            parent = item["grpc"]["RunQuery"]["parent"]
            field_path = item["grpc"]["RunQuery"]["field_path"]
                
            fields = {}
            fields = query_pb2.StructuredQuery.FieldReference(field_path = field_path)
            select = query_pb2.StructuredQuery.Projection(fields = [fields])
                  
            structured_query = query_pb2.StructuredQuery(select=select)

            run_query_request = firestore_pb2.RunQueryRequest(parent=parent, structured_query=structured_query)
            run_query_response = stub.RunQuery(run_query_request)

            print('starting read from batch: ', type(run_query_response))
            for runquery in run_query_response:
                print(runquery)

if __name__ == "__main__":
    main()
