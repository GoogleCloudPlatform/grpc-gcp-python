# Copyright 2018 gRPC-GCP authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import setuptools
import version

LICENSE = 'Apache License 2.0'

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'License :: OSI Approved :: Apache Software License',
]

setuptools.setup(
    name='grpcio-gcp',
    version=version.GRPC_GCP,
    description='gRPC extensions for Google Cloud Platform',
    author='The gRPC-GCP Authors',
    author_email='grpc-io@googlegroups.com',
    url='https://grpc.io',
    long_description=open('README.rst').read(),
    license=LICENSE,
    classifiers=CLASSIFIERS,
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        'grpcio>={version}'.format(version=version.GRPC),
    ],
)
