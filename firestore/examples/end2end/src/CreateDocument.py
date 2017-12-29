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

            creds = item["grpc"]["CreateDocument"]["credentials"]

            credentials = service_account.Credentials.from_service_account_file("{}".format(creds))
            scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
            http_request = google.auth.transport.requests.Request()
            channel = google.auth.transport.grpc.secure_authorized_channel(scoped_credentials, http_request, 'firestore.googleapis.com:443')

            stub = firestore_pb2_grpc.FirestoreStub(channel)

            # parent defined in the grpc.json file
            parent = item["grpc"]["CreateDocument"]["parent"] 

            # collection_id defined in the grpc.json
            collection_id = item["grpc"]["CreateDocument"]["collection_id"]
             
            document_id = item["grpc"]["CreateDocument"]["document_id"]

            if document_id == ' ':

                document_id = raw_input("Please provde a document_id: \n")
             
            document = {}
             
            create_document_request = firestore_pb2.CreateDocumentRequest(parent=parent,collection_id=collection_id, document_id=document_id, document=document)
            document = stub.CreateDocument(create_document_request)
            print(document)
                      

if __name__ == "__main__":
    main()
