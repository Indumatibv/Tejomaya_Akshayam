#!/usr/bin/env python
import schedule
import time
import subprocess
import logging
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
AGENTS_DIR = BASE_DIR / "agents"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_DIR / "scheduler.log"), logging.StreamHandler()]
)

PYTHON = sys.executable
ORCHESTRATOR = str(AGENTS_DIR / "run_agents.py")


def job():
    logging.info("⏳ Trigger received: running ETL pipeline")
    subprocess.Popen([PYTHON, ORCHESTRATOR])
    logging.info("🚀 ETL pipeline started")


schedule.every().day.at("11:49").do(job)

logging.info("Scheduler started... waiting for triggers")

while True:
    schedule.run_pending()
    time.sleep(1)
