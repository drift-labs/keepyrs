# Drift Protocol v2 Python Keeper Bots

## Set up

this repository uses python 3.10 and [poetry](https://python-poetry.org/)

after install python 3.10, you can setup a virtual env setup
```
python3.10 -m venv venv
source venv/bin/activate
pip install poetry
```

1) Run `poetry shell`
2) Run `poetry install`
3) Create `.env` file in `keepyrs` with wallet `PRIVATE_KEY` and your `RPC_URL`

## JIT MAKER

`poetry run python -m jit_maker.src.jit_maker`

You CANNOT make multiple markets with the same sub account on the JIT Maker.  This code does not account for overleveraging as a result of having positions open across several markets on the same sub account id. 
