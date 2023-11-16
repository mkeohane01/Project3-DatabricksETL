install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main 

format:	
	black *.py 

# lint:
# 	ruff ETL-notebooks

run-workflow:
	python main.py