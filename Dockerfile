FROM python:3.10-buster
RUN pip install poetry==1.6

WORKDIR /app
COPY pyproject.toml poetry.lock /app
COPY README.md /app
COPY ./src /app/src
RUN poetry config virtualenvs.create false
RUN poetry install --only main
ENTRYPOINT ["poetry", "run", "diff"] 
