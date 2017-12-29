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

#from google.firestore.v1beta1 import firestore_pb2
#from google.firestore.v1beta1 import firestore_pb2_grpc
#from google.firestore.v1beta1 import document_pb2
#from google.firestore.v1beta1 import document_pb2_grpc
#from google.firestore.v1beta1 import common_pb2
#from google.firestore.v1beta1 import common_pb2_grpc
#from google.firestore.v1beta1 import write_pb2
#from google.firestore.v1beta1 import write_pb2_grpc
#from google.protobuf import empty_pb2
#from google.protobuf import timestamp_pb2

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

            creds = item["grpc"]["GetIndex"]["credentials"]

            credentials = service_account.Credentials.from_service_account_file("{}".format(creds))
            scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
            http_request = google.auth.transport.requests.Request()
            channel = google.auth.transport.grpc.secure_authorized_channel(scoped_credentials, http_request, 'firestore.googleapis.com:443')


            stub = firestore_admin_pb2_grpc.FirestoreAdminStub(channel)

            parent = item["grpc"]["GetIndex"]["parent"] 
                    
            list_indexes_request = firestore_admin_pb2.ListIndexesRequest(parent=parent)
            list_indexes_response = {}
            list_indexes_response = stub.ListIndexes(list_indexes_request)
                  
            print("\n")
            for index in list_indexes_response.indexes:
                print(index)
                   
            print("\n") 

            name = raw_input("Please enter the index to get: \n")


            stub = firestore_admin_pb2_grpc.FirestoreAdminStub(channel)

            get_index_request = firestore_admin_pb2.GetIndexRequest(name=name)
            get_index_response = stub.GetIndex(get_index_request)

            print(get_index_response)


if __name__ == "__main__":
    main()
