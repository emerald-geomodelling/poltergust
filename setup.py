#!/usr/bin/env python

import setuptools

setuptools.setup(
    name='luigi-poltergust',
    version='0.0.4',
    description='Task trigger for Luigi',
    long_description='''Trigger Luigi tasks on multiple worker
machines. Python modules for tasks and their dependencies are
installed automatically in virtualenvs on each worker.
''',
    long_description_content_type="text/markdown",
    author='Egil Moeller',
    author_email='em@emrld.no',
    url='https://github.com/emerald-geomodelling/poltergust',
    packages=setuptools.find_packages(),
    install_requires=[
        "luigi",
        "pieshell",
        "pyyaml",
        "virtualenv",
        "requests",
        "google-auth",
        "google-api-python-client",
        "poltergust-luigi-utils>=0.0.7"
    ],
)
