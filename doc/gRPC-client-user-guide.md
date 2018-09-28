# Instructions for create a gRPC client for google cloud services

## Overview

This instruction includes a step by step guide for creating a gRPC 
client to test the google cloud service from an empty linux 
VM, using GCE ubuntu 16.04 TLS instance.

The main steps are followed as steps below: 

- Environment Prerequisites
- Install gRPC-python, plugin and cloud API
- Generate client API from .proto files
- Create the client and send/receive RPC.

## Environment Prerequisite

## Install gRPC-python, plugin and cloud API
```sh
$ [sudo] apt-get install python-pip
# Install gRPC-python
$ [sudo] pip install grpcio
# Install gRPC-python plugin, which is used to generate pb files
$ [sudo] pip install grpcio-tools
# install google auth
$ [sudo] pip install google-auth
```

## Generate client API from .proto files 
The plugin is installed with [grpcio-tools](https://grpc.io/docs/tutorials/basic/python.html#generating-client-and-server-code).
The command using plugin looks like
```sh
$ mkdir project-python && cd project-python
$ python -m grpc_tools.protoc --proto_path=/path/to/your/proto_path --python_out=./ \
--grpc_python_out=./ \
path/to/your/proto_dependency_directory1/*.proto \
path/to/your/proto_dependency_directory2/*.proto \
path/to/your/proto_service_directory/*.proto
```

Take firestore API inside [googleapis github repo](https://github.com/googleapis/googleapis)
as example. The `proto` files needed are:
```
google/api/annotations.proto
google/api/http.proto
google/api/httpbody.proto
google/longrunning/operations.proto
google/rpc/code.proto
google/rpc/error_details.proto
google/rpc/status.proto google/type/latlng.proto
google/firestore/v1beta1/firestore.proto
google/firestore/v1beta1/common.proto
google/firestore/v1beta1/query.proto
google/firestore/v1beta1/write.proto
google/firestore/v1beta1/document.proto.
```
The command generate client will be
```
python -m grpc_tools.protoc --proto_path=googleapis --python_out=./ --grpc_python_out=./ \
google/api/annotations.proto google/api/http.proto google/api/httpbody.proto \
google/longrunning/operations.proto  google/rpc/code.proto google/rpc/error_details.proto  \
google/rpc/status.proto google/type/latlng.proto google/firestore/v1beta1/firestore.proto \
google/firestore/v1beta1/common.proto google/firestore/v1beta1/query.proto \
google/firestore/v1beta1/write.proto google/firestore/v1beta1/document.proto
```

The client API library is generated under `project-python`.
Take [`Firestore`](https://github.com/googleapis/googleapis/blob/master/google/firestore/v1beta1/firestore.proto)
as example, the Client API is under
`project-python/google/firestore/v1beta1` depends on your
package namespace inside .proto file. An easy way to find your client is
```sh
$ cd project-python
$ find ./ -name [service_name: eg, firestore, cluster_service]*
```

## Create the client and send/receive RPC.
Now it's time to use the client API to send and receive RPCs.

**Set credentials file**

This is important otherwise your RPC response will be a permission error.
``` sh
$ vim $HOME/key.json
## Paste you credential file downloaded from your cloud project
## which you can find in APIs&Services => credentials => create credentials
## => Service account key => your credentials
$ export GOOGLE_APPLICATION_CREDENTIALS=$HOME/key.json
```

**Implement Service Client**

Take a unary-unary RPC `listDocument` from `FirestoreClient` as example.
Create a file name `list_document_client.py`.
- Create `__init__.py`. You need to create `__init__.py` to let your python
script find the module.
```
$ vim google/__init__.py
$ vim google/firestore/__init__.py
$ vim google/firestore/v1beta1/__init__.py
$ vim google/firestore/rpc/__init__.py
$ vim google/firestore/api/__init__.py
$ vim google/firestore/type/__init__.py
$ vim google/firestore/longrunning/__init__.py
```
- Import library
```
from google import auth as google_auth
from google.auth.transport import requests as google_auth_transport_requests
from google.auth.transport import grpc as google_auth_transport_grpc
from google.firestore.v1beta1 import firestore_pb2
from google.firestore.v1beta1 import firestore_pb2_grpc
```
- Set Google Auth. Please see the referece for 
[authenticate with Google using an Oauth2 token](https://grpc.io/docs/guides/auth.html#python)
```
scoped_credentials, _ = google_auth.default(scopes=('https://www.googleapis.com/auth/datastore',))
request = google_auth_transport_requests.Request()
channel = google_auth_transport_grpc.secure_authorized_channel(
scoped_credentials, request, 'firestore.googleapis.com:443')
```
There is an optional way to set the key.json without export 
`GOOGLE_APPLICATION_CREDENTIALS`
```
credentials = service_account.Credentials.from_service_account_file("/path/to/key.json")
scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/datastore'])
request = google_auth_transport_requests.Request()
channel = google_auth_transport_grpc.secure_authorized_channel(
scoped_credentials, request, 'firestore.googleapis.com:443')
```

- Create Stub
```
stub = firestore_pb2_grpc.FirestoreStub(channel)
```
- Invoke RPC
```
list_document_request = firestore_pb2.ListDocumentsRequest(
parent = 'projects/xxxxx/databases/(default)/documents')
list_document_response = stub.ListDocuments(list_document_request)
```
- Print RPC response
```
print(list_document_response)
```
- Run the script
```sh
$ python list_document_client.py
```

For different kinds of RPC(unary-unary, unary-stream, stream-unary, stream-stream),
please check [grpc.io Python part](https://grpc.io/docs/tutorials/basic/python.html#simple-rpc)
for reference.
