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

            creds = item["grpc"]["CreateIndex"]["credentials"]

            credentials = service_account.Credentials.from_service_account_file("{}".format(creds))
            scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
            http_request = google.auth.transport.requests.Request()
            channel = google.auth.transport.grpc.secure_authorized_channel(scoped_credentials, http_request, 'firestore.googleapis.com:443')


            stub = firestore_admin_pb2_grpc.FirestoreAdminStub(channel)

            # name, parent and collection_id are definded in the grpc.json file
            name = item["grpc"]["CreateIndex"]["name"]
            parent = item["grpc"]["CreateIndex"]["parent"]
            collection_id = item["grpc"]["CreateIndex"]["collection_id"]
              
            fields = []
                     
            # field_path1 and mode1 are defined in the grpc.json file         
            field_path1= item["grpc"]["CreateIndex"]["field_path1"] 
            mode1= item["grpc"]["CreateIndex"]["mode1"]

            fields1 = index_pb2.IndexField(field_path=field_path1, mode=mode1)

                  
            # field_path2 and mode2 are defined in the grpc.json file         
            field_path2= item["grpc"]["CreateIndex"]["field_path2"] 
            mode2= item["grpc"]["CreateIndex"]["mode2"]
                     
            fields2 = index_pb2.IndexField(field_path=field_path2, mode=mode2)

            fields = [fields1, fields2]
                     
            index = index_pb2.Index(name=name, collection_id=collection_id, fields=fields)

            create_index_request = firestore_admin_pb2.CreateIndexRequest(parent=parent, index=index)
            create_index_response = stub.CreateIndex(create_index_request)

            print(create_index_response)

if __name__ == "__main__":
    main()
