#!/usr/bin/env python
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="target-sqlite",
    version="0.3.0",
    author="Meltano",
    author_email="meltano@gitlab.com",
    description="Singer.io target for importing data to SQLite",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/meltano/target-sqlite",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "inflection>=0.3.1",
        "singer-python>=5.0.12",
        "sqlalchemy==1.3.0",
    ],
    extras_require={
        "dev": [
            "pytest>=3.8",
            "black>=18.3a0",
        ]
    },
    entry_points="""
    [console_scripts]
    target-sqlite=target_sqlite:main
    """,
    python_requires=">=3.7",
    packages=find_packages(exclude=["tests"]),
    package_data={},
    include_package_data=True,
)
