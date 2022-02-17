import os
import re
from setuptools import setup, find_packages


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'aiomysql', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in aiomysql/__init__.py')


setup(version=read_version(),
      long_description='\n\n'.join((read('README.rst'), read('CHANGES.txt'))),
      packages=find_packages(exclude=['tests', 'tests.*']))
