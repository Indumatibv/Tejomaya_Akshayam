from fastapi import FastAPI
import subprocess
import sys
from pathlib import Path

app = FastAPI()

PYTHON = sys.executable
BASE_DIR = Path(__file__).resolve().parent.parent
AGENTS_DIR = BASE_DIR / "agents"

SEARCH_AGENT = str(AGENTS_DIR / "searching_agent.py")
PARSE_AGENT = str(AGENTS_DIR / "parsing_agent.py")
RUN_AGENTS = str(AGENTS_DIR / "run_agents.py")


@app.get("/")
def root():
    return {"status": "Tejomaya ETL Pipeline API running"}


@app.post("/run-search")
def run_search():
    subprocess.Popen([PYTHON, SEARCH_AGENT])
    return {"message": "Searching agent started"}


@app.post("/run-parse")
def run_parse():
    subprocess.Popen([PYTHON, PARSE_AGENT])
    return {"message": "Parsing agent started"}


@app.post("/run-all")
def run_all():
    subprocess.Popen([PYTHON, RUN_AGENTS])
    return {"message": "Both agents started sequentially"}
