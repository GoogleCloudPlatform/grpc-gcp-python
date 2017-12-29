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
from google.protobuf import empty_pb2
from google.protobuf import timestamp_pb2

   
def main():

   subprocess.call('clear')

   fl = os.path.dirname(os.path.abspath(__file__))
   fn = os.path.join(fl, 'grpc.json')

   with open(fn) as grpc_file:
          
            item = json.load(grpc_file)

            creds = item["grpc"]["GetDocument"]["credentials"]

            credentials = service_account.Credentials.from_service_account_file("{}".format(creds))
            scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
            http_request = google.auth.transport.requests.Request()
            channel = google.auth.transport.grpc.secure_authorized_channel(scoped_credentials, http_request, 'firestore.googleapis.com:443')
              
            stub = firestore_pb2_grpc.FirestoreStub(channel)

            now = time.time()
            seconds = int(now)
            timestamp = timestamp_pb2.Timestamp(seconds=seconds)

            field_paths = {}

            # field_paths is set in the grpc.json file
            field_paths=item["grpc"]["GetDocument"]["document_mask_field_path"] 

            mask = common_pb2.DocumentMask(field_paths = [field_paths])


            # name is set in the grpc.json file
            name = item["grpc"]["GetDocument"]["name"]  

            if name == ' ' :

                name = raw_input("Please enter the resource name of the Document to get, e.g. projects/{project_id}/databases/{database_id}/documents/{document_path}: \n")
                        
                        
            get_document_request = firestore_pb2.GetDocumentRequest(name=name, mask=mask)
            get_document_response = stub.GetDocument(get_document_request)
                     
            print(get_document_response)

if __name__ == "__main__":
    main()
