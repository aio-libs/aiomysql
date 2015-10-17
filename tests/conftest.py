import sys


def pytest_ignore_collect(path, config):
    if 'pep492' in str(path):
        if sys.version_info < (3, 5, 0):
            return True
