import os
from setuptools import setup


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


setup(
    long_description='\n\n'.join((read('README.rst'), read('CHANGES.txt'))),
)
