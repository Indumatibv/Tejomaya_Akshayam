#!/usr/bin/env python
import subprocess
import time
import logging
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_DIR / "run_agents.log"), logging.StreamHandler()]
)

PYTHON = sys.executable
SEARCH_AGENT = str(BASE_DIR / "agents" / "searching_agent.py")
PARSE_AGENT = str(BASE_DIR / "agents" / "parsing_agent.py")

SEARCH_LOG = str(LOG_DIR / "searching_agent.log")
PARSE_LOG = str(LOG_DIR / "parsing_agent.log")

EXCEL_OUTPUT = BASE_DIR / "data" / "weekly_sebi_downloads.xlsx"


def run_process(script_path, log_path):
    logging.info(f"🚀 Running script → {script_path}")
    with open(log_path, "a") as f:
        f.write(f"\n\n=== Run at {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        result = subprocess.run([PYTHON, script_path], stdout=f, stderr=f)
    logging.info(f"✔ Completed with returncode={result.returncode}")
    return result.returncode


def main():
    logging.info("===== Starting 2-Agent ETL Chain =====")

    if run_process(SEARCH_AGENT, SEARCH_LOG) != 0:
        logging.error("❌ Searching agent failed")
        return

    if not EXCEL_OUTPUT.exists():
        logging.error(f"❌ Excel not generated: {EXCEL_OUTPUT}")
        return

    logging.info(f"📄 Excel found: {EXCEL_OUTPUT}")

    if run_process(PARSE_AGENT, PARSE_LOG) != 0:
        logging.error("❌ Parsing agent failed")
        return

    logging.info("🎉 BOTH AGENTS COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    main()
