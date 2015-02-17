import os
import re
import sys
from setuptools import setup, find_packages


install_requires = ['PyMySQL<=0.6.3']

PY_VER = sys.version_info

if PY_VER >= (3, 4):
    pass
elif PY_VER >= (3, 3):
    install_requires.append('asyncio')
else:
    raise RuntimeError("aiomysql doesn't suppport Python earllier than 3.3")


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()

extras_require = {'sa': ['sqlalchemy>=0.9'], }


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

classifiers = [
    'License :: OSI Approved :: MIT License',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Operating System :: POSIX',
    'Environment :: Web Environment',
    'Development Status :: 3 - Alpha',
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
]


setup(name='aiomysql',
      version=read_version(),
      description=('MySQL driver for asyncio.'),
      long_description='\n\n'.join((read('README.rst'), read('CHANGES.txt'))),
      classifiers=classifiers,
      platforms=['POSIX'],
      author="Nikolay Novik",
      author_email="nickolainovik@gmail.com",
      url='https://github.com/aio-libs/aiomysql',
      download_url='https://pypi.python.org/pypi/aiomysql',
      license='MIT',
      packages=find_packages(),
      install_requires=install_requires,
      extras_require=extras_require,
      include_package_data = True)
