FROM python:3.11-buster
RUN pip install poetry==1.6

WORKDIR /app
COPY pyproject.toml poetry.lock ./
COPY README.md .
COPY src ./src
RUN poetry config virtualenvs.create false
RUN poetry install --only main
ENTRYPOINT ["poetry", "run", "python", "-m", "pudl_output_differ.main"]
