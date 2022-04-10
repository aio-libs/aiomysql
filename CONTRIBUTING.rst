Contributing
============

.. _GitHub: https://github.com/aio-libs/aiomysql

Thanks for your interest in contributing to ``aiomysql``, there are multiple
ways and places you can contribute.

Reporting an Issue
------------------
If you have found issue with `aiomysql` please do
not hesitate to file an issue on the GitHub_ project. When filing your
issue please make sure you can express the issue with a reproducible test
case.

When reporting an issue we also need as much information about your environment
that you can include. We never know what information will be pertinent when
trying narrow down the issue. Please include at least the following
information:

* Version of `aiomysql` and `python`.
* Version of MySQL/MariaDB.
* Platform you're running on (OS X, Linux, Windows).


Instructions for contributors
-----------------------------


In order to make a clone of the GitHub_ repo: open the link and press the
"Fork" button on the upper-right menu of the web page.

I hope everybody knows how to work with git and github nowadays :)

Workflow is pretty straightforward:

  1. Clone the GitHub_ repo

  2. Make a change

  3. Make sure all tests passed

  4. Commit changes to own aiomysql clone

  5. Make pull request from github page for your clone

Preconditions for running aiomysql test suite
---------------------------------------------

We expect you to use a python virtual environment to run our tests.

There are several ways to make a virtual environment.

If you like to use *virtualenv* please run:

.. code-block:: sh

   $ cd aiomysql
   $ virtualenv --python="$(which python3)" venv

For standard python *venv*:

.. code-block:: sh

   $ cd aiomysql
   $ python3 -m venv venv

For *virtualenvwrapper*:

.. code-block:: sh

   $ cd aiomysql
   $ mkvirtualenv --python="$(which python3)" aiomysql

There are other tools like *pyvenv* but you know the rule of thumb
now: create a python3 virtual environment and activate it.

After that please install libraries required for development:

.. code-block:: sh

   $ pip install -r requirements-dev.txt

Congratulations, you are ready to run the test suite

Install database
----------------

Fresh local installation of `mysql` has user `root` with empty password, tests
use this values by default. But you always can override host/port, user and
password in `aiomysql/tests/base.py` file or install corresponding environment
variables. Tests require two databases to be created before running suit:

.. code-block:: sh

   $ mysql -u root
   mysql> CREATE DATABASE test_pymysql  DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;
   mysql> CREATE DATABASE test_pymysql2 DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;


Run aiomysql test suite
-----------------------

After all the preconditions are met you can run tests typing the next
command:

.. code-block:: sh

   $ make test

The command at first will run the *flake8* tool (sorry, we don't accept
pull requests with pep8 or pyflakes errors).

On *flake8* success the tests will be run.

Please take a look on the produced output.

Any extra texts (print statements and so on) should be removed.


Tests coverage
--------------

We are trying hard to have good test coverage; please don't make it worse.

Use:

.. code-block:: sh

   $ make cov

to run test suite and collect coverage information. Once the command
has finished check your coverage at the file that appears in the last
line of the output:
``open file:///.../aiomysql/coverage/index.html``

Please go to the link and make sure that your code change is covered.


Documentation
-------------

We encourage documentation improvements.

Please before making a Pull Request about documentation changes run:

.. code-block:: sh

   $ make doc

Once it finishes it will output the index html page
``open file:///.../aiomysql/docs/_build/html/index.html``.

Go to the link and make sure your doc changes looks good.

The End
-------

After finishing all steps make a GitHub_ Pull Request, thanks.
