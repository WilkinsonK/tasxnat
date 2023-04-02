import pathlib
from setuptools import setup, find_packages

PROJECT_ROOT = pathlib.Path(__file__).parent

# Configuration
VERSION = (0, 0, 3)
DESCRIPTION = "Simple tasking interface."
LONG_DESCRIPTION = """\
Allows users to create tasks out of callables.
"""
DEPENDENCIES = []


setup(
    name="tasxnat",
    version=".".join([str(v) for v in VERSION]),
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author="Keenan W. Wilkinson",
    author_email="keenanwilkinson@outlook.com",
    license="MIT",
    packages=find_packages(),
    install_requires=DEPENDENCIES,
    keywords=["tasks"],
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ]
)
