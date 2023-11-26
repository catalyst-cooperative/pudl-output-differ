FROM python:3.10-buster
RUN pip install poetry==1.6

# TODO(rousik): Presumably, when we run this tool, we will want to
# either mount remote paths as "local" directories, or cache the remote
# files locally using plentiful storage.

# --cache-dir argument can be used to control where things are cached.


WORKDIR /app
COPY pyproject.toml poetry.lock /app
COPY README.md /app
COPY ./src /app/src
RUN poetry config virtualenvs.create false
RUN poetry install --only main
ENTRYPOINT ["poetry", "run", "diff"] 
