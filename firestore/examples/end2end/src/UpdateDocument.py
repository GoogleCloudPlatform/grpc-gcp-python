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


   fl = os.path.dirname(os.path.abspath(__file__))
   fn = os.path.join(fl, 'grpc.json')

   with open(fn) as grpc_file:
          
            item = json.load(grpc_file)

            creds = item["grpc"]["UpdateDocument"]["credentials"]

            credentials = service_account.Credentials.from_service_account_file("{}".format(creds))
            scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
            http_request = google.auth.transport.requests.Request()
            channel = google.auth.transport.grpc.secure_authorized_channel(scoped_credentials, http_request, 'firestore.googleapis.com:443')


            stub = firestore_pb2_grpc.FirestoreStub(channel)

            now = time.time()
            seconds = int(now)
            timestamp = timestamp_pb2.Timestamp(seconds=seconds)


            # name is set in the grpc.json file
            name = item["grpc"]["UpdateDocument"]["name"]  

            if name == ' ':
                name=raw_input("Please provide the resource name of the document to be updated: \n")
                                                                                                          
            field=raw_input('Please provide the field to be updated, e.g. "foo" ')
            current_value=raw_input('Please provide the current value of the field to be updated, e.g. "bar"  ')
                      
            update_mask = common_pb2.DocumentMask(field_paths = [field, current_value])
               
            value=raw_input("Please provide the new value of the field to update using the following syntax, e.g. 'foo_boo' \n")

            value_ = document_pb2.Value(string_value = value)

            document = document_pb2.Document(name=name, fields={field:value_})  
               
            update_document_request = firestore_pb2.UpdateDocumentRequest(document=document, update_mask=common_pb2.DocumentMask(field_paths = [field]))  
            update_document_response = stub.UpdateDocument(update_document_request)

            print(update_document_response)

if __name__ == "__main__":
    main()
