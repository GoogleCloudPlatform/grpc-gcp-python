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

     creds = item["grpc"]["BatchGetDocuments"]["credentials"]

     credentials = service_account.Credentials.from_service_account_file("{}".format(creds))
     scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
     http_request = google.auth.transport.requests.Request()
     channel = google.auth.transport.grpc.secure_authorized_channel(scoped_credentials, http_request, 'firestore.googleapis.com:443')

     stub = firestore_pb2_grpc.FirestoreStub(channel)
     
     # database and documents defined in the grpc.json file
     database = item["grpc"]["BatchGetDocuments"]["database"]
     documents = item["grpc"]["BatchGetDocuments"]["documents"]

     if documents == ' ':

        print("Please enter the document Id's you would like to retrieve, e.g.\n")
        print("projects/geoff-python/databases/(default)/documents/users/alovelace \n") 
      
        documents = raw_input(":")

     batch_get_document_request = firestore_pb2.BatchGetDocumentsRequest(database = database, documents = [documents])
     batch_get_document_response = stub.BatchGetDocuments(batch_get_document_request)
                     
     print('staring read from batch: ', type(batch_get_document_response))
     for get_document in batch_get_document_response:
       	    print(get_document)


if __name__ == "__main__":
    main()
