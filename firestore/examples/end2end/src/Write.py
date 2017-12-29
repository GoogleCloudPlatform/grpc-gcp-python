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

   
def first_message(database, write):
    messages = [
            firestore_pb2.WriteRequest(database = database, writes = [])
    ]
    for msg in messages:
            yield msg

def generate_messages(database, writes, stream_id, stream_token):
    # writes can be an array and append to the messages, so it can write multiple Write
    # here just write one as example
    messages = [
            firestore_pb2.WriteRequest(database=database, writes = []),
            firestore_pb2.WriteRequest(database=database, writes = [writes],  stream_id = stream_id, stream_token = stream_token) 
    ]
    for msg in messages:
            yield msg




def main():


   fl = os.path.dirname(os.path.abspath(__file__))
   fn = os.path.join(fl, 'grpc.json')

   with open(fn) as grpc_file:
          
            item = json.load(grpc_file)

            creds = item["grpc"]["Write"]["credentials"]

            credentials = service_account.Credentials.from_service_account_file("{}".format(creds))
            scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
            http_request = google.auth.transport.requests.Request()
            channel = google.auth.transport.grpc.secure_authorized_channel(scoped_credentials, http_request, 'firestore.googleapis.com:443')

            stub = firestore_pb2_grpc.FirestoreStub(channel)
            
            database = item["grpc"]["Write"]["database"]
            name = item["grpc"]["Write"]["name"]
            first_write = write_pb2.Write()

            responses = stub.Write(first_message(database, first_write))
            for response in responses:
                print("Received message %s" % (response.stream_id))
                print(response.stream_token)

            value_ = document_pb2.Value(string_value = "foo_boo")
            update = document_pb2.Document(name=name, fields={"foo":value_})  
            writes  = write_pb2.Write(update_mask=common_pb2.DocumentMask(field_paths = ["foo"]), update=update)
            r2 = stub.Write(generate_messages(database, writes, response.stream_id, response.stream_token))
            for r in r2:
                print(r.write_results)

if __name__ == "__main__":
    main()
