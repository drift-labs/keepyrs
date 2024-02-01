FROM python:3.10.13-bullseye
RUN pip install poetry
COPY . .
RUN poetry install
