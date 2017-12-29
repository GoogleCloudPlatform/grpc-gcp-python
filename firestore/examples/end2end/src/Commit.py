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

from google.firestore.admin.v1beta1 import index_pb2
from google.firestore.admin.v1beta1 import index_pb2_grpc
from google.firestore.admin.v1beta1 import firestore_admin_pb2
from google.firestore.admin.v1beta1 import firestore_admin_pb2_grpc
   
   
       
def main():

   subprocess.call('clear')

   fl = os.path.dirname(os.path.abspath(__file__))
   fn = os.path.join(fl, 'grpc.json')

   with open(fn) as grpc_file:
          
            item = json.load(grpc_file)

            creds = item["grpc"]["Commit"]["credentials"]

            credentials = service_account.Credentials.from_service_account_file("{}".format(creds))
            scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
            http_request = google.auth.transport.requests.Request()
            channel = google.auth.transport.grpc.secure_authorized_channel(scoped_credentials, http_request, 'firestore.googleapis.com:443')

            stub = firestore_pb2_grpc.FirestoreStub(channel)

            now = time.time()
            seconds = int(now)
            timestamp = timestamp_pb2.Timestamp(seconds=seconds)

            # database defined in the grpc.json file      
	    database = item["grpc"]["Commit"]["database"]

            options = common_pb2.TransactionOptions(read_write = common_pb2.TransactionOptions.ReadWrite()) 
       	    begin_transaction_request = firestore_pb2.BeginTransactionRequest(database = database, options = options)
       	    begin_transaction_response = stub.BeginTransaction(begin_transaction_request)
            transaction = begin_transaction_response.transaction

            stub = firestore_pb2_grpc.FirestoreStub(channel)
                  
            now = time.time()
            seconds = int(now)
            timestamp = timestamp_pb2.Timestamp(seconds=seconds)
                    
            field_paths = {}
            # document mask field_path is defined in the grpc.json file
            field_paths= item["grpc"]["Commit"]["field_paths"] 
            update_mask = common_pb2.DocumentMask(field_paths = [field_paths])
                      
            # document_fileds is defined in the grpc.json file           
            fields=item["grpc"]["Commit"]["fields"] 
                     
            # document_name is defined in the grpc.json file
            name =item["grpc"]["Commit"]["name"]

            update = document_pb2.Document(name=name, fields=fields, create_time = timestamp, update_time = timestamp)  

            writes = {}
	    database = item["grpc"]["Commit"]["database"]
            writes = write_pb2.Write(update_mask = update_mask, update=update)
            
       	    commit_request = firestore_pb2.CommitRequest(database = database, writes = [writes], transaction = transaction )
            commit_response = stub.Commit(commit_request)

            print(commit_response)


if __name__ == "__main__":
    main()
