# Tutorial Template Project

This is a [Dagster](https://dagster.io/) project made to be used alongside the official [Dagster tutorial](https://docs.dagster.io/tutorial). This project is intentionally empty and is meant to be used as a starting point for the tutorial.

### Setup

1. Get example data for [BAA01
   sample](https://girder.htmdec.org/#collection/64245a5d4236ff9b0883e926/folder/wtlocal:L3Nydi9oZW1pMDEtajAxL2h0bWRlYy9UQU1VL1NhbXBsZSBEYXRhL0NhbXBhaWduMl9JdGVyYXRpb24xX0JBQS9WQU0tQS9IU1JOSS9CQUEwMXw2NWZhYzM2YTYwNjYyZWYwODRmNmJjMDY=) and place it in a folder.
2. `export DATA_PATH=/path/to/data/folder`
3. Run:

```bash
python3 -m venv venv
. ./venv/bin/activate
pip install -e ".[dev]"
dagster dev
```
