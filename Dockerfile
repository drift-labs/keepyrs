FROM python:3.10.13-bullseye
RUN pip install poetry
COPY . .
RUN poetry install
CMD ["poetry", "run", "python", "-m", "jit_maker.src.jit_maker"]