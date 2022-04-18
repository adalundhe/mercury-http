import os
from setuptools import (
    setup,
    find_packages
)

current_directory = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(current_directory, 'README.md'), "r") as readme:
    package_description = readme.read()

setup(
    name="mercury-http",
    version="0.4.0",
    description="Performant HTTP client.",
    long_description=package_description,
    long_description_content_type="text/markdown",
    author="Sean Corbett",
    author_email="sean.corbett@umconnect.edu",
    url="https://github.com/scorbettUM/mercury-http",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    install_requires=[
        'aiodns',
        'h2',
        'graphql-core',
        'py3-async-tools'
    ],
    python_requires='>=3.8'
)
