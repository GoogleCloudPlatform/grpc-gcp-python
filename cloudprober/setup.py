"""Setup file for grpc-gcp-prober package.

This specifies all the dependencies to install the module for python prober.
"""

import setuptools

LICENSE = 'Apache License 2.0'

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
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
    name='grpc_gcp_prober',
    version='0.0.1',
    description='Prober scripts for cloud APIs in Python',
    author='Weiran Fang',
    author_email='weiranf@google.com',
    url='https://grpc.io',
    license=LICENSE,
    classifiers=CLASSIFIERS,
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=['grpcio', 'google-auth', 'requests'],
)
