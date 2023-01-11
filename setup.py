#!/usr/bin/env python3

import os
from setuptools import setup, find_packages
import sys


package_dir = os.path.dirname(__file__)
requirements_file_path = os.path.join(package_dir, "requirements.in")

with open(requirements_file_path, "r") as f:
    packages = [f.rstrip() for f in f.readlines()]

with open("README.md") as f:
    long_description = f.read()

setup(
    name="cellsite",
    version="1.2.3",
    description="library for cell site calculations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NetherlandsForensicInstitute/cellsite",
    author="Netherlands Forensic Institute",
    author_email="fbda@nfi.nl",
    packages=find_packages(),
    setup_requires=["nose"],
    test_suite="nose.collector",
    install_requires=packages,
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
