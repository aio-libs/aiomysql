# Some simple testing tasks (sorry, UNIX only).

FLAGS=

checkrst:
	python setup.py check --restructuredtext

pyroma:
	pyroma -d .


flake:checkrst pyroma
	flake8 aiomysql tests examples

test: flake
	py.test -s $(FLAGS) ./tests/

vtest:
	py.test -s -v $(FLAGS) ./tests/


cov cover coverage: flake
	py.test -s -v  --cov-report term --cov-report html --cov aiomysql ./tests
	@echo "open file://`pwd`/htmlcov/index.html"

clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name '@*' `
	rm -f `find . -type f -name '#*#' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -f .coverage
	rm -rf coverage
	rm -rf build
	rm -rf htmlcov
	rm -rf dist

start_mysql:
	@echo "----------------------------------------------------"
	@echo "Starting mysql, see docker-compose.yml for user/pass"
	@echo "----------------------------------------------------"
	docker-compose -f docker-compose.yml up -d mysql

stop_mysql:
	docker-compose -f docker-compose.yml stop mysql

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: all flake test vtest cov clean doc
