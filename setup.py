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
    version="0.1.0",
    author="Sean Corbett",
    author_email="sean.corbett@umconnect.edu",
    description="Concurrent HTTP 1.X client.",
    long_description=package_description,
    long_description_content_type="text/markdown",
    url="https://github.com/scorbettUM/mercury-client",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    install_requires=[
        'aiodns',
        'py3-async-tools'
    ],
    python_requires='>=3.6'
)
