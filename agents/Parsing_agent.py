#!/usr/bin/env python
# agents/parsing_agent.py
# ============================================================
# REGULATIONS-ONLY PARSING & SUMMARY (TEJOMAYA v1)
# ============================================================

import os
import re
import logging
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from unstructured.partition.pdf import partition_pdf
from openpyxl import load_workbook, Workbook
from datetime import datetime
import torch
import warnings
import time

from langchain.llms import Ollama
from langchain.prompts import PromptTemplate

# ---------------------- Logging ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

warnings.filterwarnings("ignore")
load_dotenv()

# ---------------------- GPU Detection ----------------------
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
os.environ["OLLAMA_USE_GPU"] = "1" if device.type == "cuda" else "0"

logging.info(f"Using device → {device}")

# ---------------------- LLM ----------------------
llm = Ollama(model="mistral:latest")

# ============================================================
# REGULATIONS SUMMARY PROMPT (FINAL, AUTHORITATIVE)
# ============================================================

REGULATIONS_PROMPT = PromptTemplate(
    template="""
You are a senior regulatory analyst preparing a concise, client-ready summary of a SEBI regulation document
for business stakeholders and compliance teams.

This document establishes a regulatory framework and contains detailed legal provisions.
The summary must allow the reader to understand what the regulation governs without opening the document.

Focus ONLY on:
- What the regulation governs at a high level (e.g., REITs, InvITs, intermediaries, market participants)
- Who it applies to (e.g., sponsors, managers, trustees, listed entities, intermediaries)
- The key compliance and governance areas covered (e.g., registration, listing, valuation, disclosures, reporting)
- Core responsibilities imposed on regulated entities
- The overall regulatory intent and scope (e.g., transparency, investor protection, market integrity)

Do NOT:
- List clauses, chapters, or definitions
- Mention page counts, circular numbers, or legal citations
- Explain individual amendments in detail

Write a clear, professional, and self-contained summary.
Limit strictly to 5–6 sentences.

Text:
{text}

Final Summary (entire answer MUST be inside double quotes):
""",
    input_variables=["text"]
)

# ============================================================
AMENDMENT_REGULATIONS_PROMPT = PromptTemplate(
    template="""
You are a senior regulatory analyst preparing a client-ready summary of a SEBI amendment regulation.

The reader is a business or compliance professional who will NOT read the original document.
The summary must clearly communicate what the amendment does in practical terms.

STRUCTURE (MANDATORY):
- Output ONLY bullet points (no paragraph introduction)
- The FIRST bullet MUST start with: “The amendment”
- The FIRST bullet should give a one-line overview of the nature of changes
- ALL remaining bullets must describe specific changes
- Remaining bullets MUST NOT start with headings, labels, or repeated phrases

CONTENT RULES (STRICT):
- Each bullet must state the outcome or impact of the change, not the legal wording
- Do NOT use phrases like:
  “definition of”, “the term”, “has been defined”, “has been introduced”
- Do NOT use colon-style labels (e.g., “Change in control:”)
- Do NOT quote or paraphrase legal definitions verbatim
- Avoid procedural or audit mechanics unless they materially affect compliance

FORMAT RULES (STRICT):
- Use ONLY black bullet points “•”
- Write a maximum of 6 bullet points
- Each bullet must be ONE concise sentence
- Do NOT use numbering (1., 2., 3.) or dashes (-)

TONE:
- Plain, professional, and client-facing
- Written like a regulatory update shared with senior stakeholders
- Outcome-focused and easy to scan

Text:
{text}

Final Summary (use ONLY black bullet points “•”):
""",
    input_variables=["text"]
)

CIRCULARS_PROMPT = PromptTemplate(
    template="""
You are a regulatory analyst writing a very short, client-ready summary of a SEBI circular.

The output MUST strictly match the following pattern:
- EXACTLY TWO bullet points
- BOTH lines must be bullet points
- The FIRST bullet MUST start with: “The Circular”

HARD RULES (NON-NEGOTIABLE):
- Output ONLY two bullet points, nothing else
- Do NOT add headings, introductions, or explanations
- If you output more or fewer bullets, the answer is INVALID

CONTENT RULES (STRICT):
- Summarise ONLY:
  • the core requirement or mandate
  • the practical compliance impact
- Mention affected entities only if essential
- Do NOT include:
  • internal processes
  • committee names
  • reporting lines
  • appointment mechanics
  • legal citations, dates, or circular numbers
- Do NOT explain background or intent explicitly

FORMAT RULES:
- Use ONLY black bullet points “•”
- Each bullet must be ONE clear sentence
- No colons, no numbering, no sub-bullets

STYLE:
- High-level
- Client-facing
- Similar in tone and length to:
  “The Circular mandates risk disclosures at every client login.”

Text:
{text}

Final Summary:
""",
    input_variables=["text"]
)


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_EXCEL_DIR = DATA_DIR / "output_excels"
OUTPUT_EXCEL_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# WEEKLY FOLDER
# ============================================================

def is_amendment_regulation(text: str) -> bool:
    return bool(re.search(r'\bamendment\b', text, re.IGNORECASE))

def get_week_folder():
    import json

    week_json = DATA_DIR / "week_range.json"
    if not week_json.exists():
        raise RuntimeError("week_range.json missing. Run searching_agent first.")

    with open(week_json, "r") as f:
        week = json.load(f)

    ws = datetime.strptime(week["week_start"], "%Y-%m-%d")
    we = datetime.strptime(week["week_end"], "%Y-%m-%d")

    folder = OUTPUT_EXCEL_DIR / f"{ws:%Y-%m-%d}_to_{we:%Y-%m-%d}"

    if folder.exists():
        import shutil
        shutil.rmtree(folder)

    folder.mkdir(parents=True)
    return folder


WEEK_FOLDER = get_week_folder()
logging.info(f"Weekly folder → {WEEK_FOLDER}")

# ============================================================
# PDF EXTRACTION (REGULATIONS-SAFE)
# ============================================================

def extract_pdf_text(pdf_path: str) -> str:
    raw = partition_pdf(
        filename=str(pdf_path),
        strategy="fast",
        include_page_breaks=False
    )

    text = "\n".join(str(el) for el in raw if el).strip()

    if not text:
        logging.info("Fallback to hi_res OCR")
        raw = partition_pdf(filename=str(pdf_path), strategy="hi_res")
        text = "\n".join(str(el) for el in raw if el).strip()

    return text


# ============================================================
# REGULATIONS TEXT FILTERING
# (THIS IS THE CORE LOGIC)
# ============================================================

def extract_regulation_core(text: str) -> str:
    lines = text.splitlines()

    keep = []
    capture = False

    CAPTURE_TRIGGERS = [
        "In exercise of the powers",
        "regulations may be called",
        "shall come into force",
        "CHAPTER",
        "Amendment",
        "Inserted",
        "Substituted",
        "Omitted"
    ]

    CONTEXT_KEYWORDS = [
        "sponsor",
        "manager",
        "trustee",
        "listing",
        "valuation",
        "disclosure",
        "investor protection"
    ]

    for line in lines:
        clean = line.strip()

        if any(k.lower() in clean.lower() for k in CAPTURE_TRIGGERS):
            capture = True

        if capture:
            # Skip pure definition enumerations
            if re.match(r'^\([a-z]+\)', clean):
                continue

            # Keep structural + context-rich lines
            if (
                any(k.lower() in clean.lower() for k in CONTEXT_KEYWORDS)
                or len(clean) > 40
            ):
                keep.append(clean)

        if len(keep) > 3000:
            break

    return "\n".join(keep)


# ============================================================
# SUMMARY GENERATION (NO CHUNKS)
# ============================================================

def generate_regulation_summary(text: str) -> str:
    core_text = extract_regulation_core(text)
    core_text = core_text[:12000]

    if is_amendment_regulation(core_text):
        prompt = AMENDMENT_REGULATIONS_PROMPT
    else:
        prompt = REGULATIONS_PROMPT

    summary = llm.invoke(
        prompt.format(text=core_text)
    ).strip()
    # summary = summary.replace("-", "•")

    return summary or "NA", core_text

# ============================================================
# EXCEL UPDATE
# ============================================================

def update_excel(row: pd.Series):
    vertical = row["Verticals"]
    sub = row["SubCategory"]

    excel_path = WEEK_FOLDER / f"{vertical}.xlsx"

    if excel_path.exists():
        wb = load_workbook(excel_path)
    else:
        wb = Workbook()
        wb.remove(wb.active)

    if sub not in wb.sheetnames:
        ws = wb.create_sheet(title=sub)
        ws.append(list(row.index))
    else:
        ws = wb[sub]

    ws.append([row.get(c, "NA") for c in row.index])
    wb.save(excel_path)
    wb.close()

    logging.info(f"Updated Excel → {excel_path} [{sub}]")


# ============================================================
# PROCESS SINGLE PDF (REGULATIONS ONLY)
# ============================================================

def process_regulation_pdf(row: pd.Series):
    pdf_path = Path(row["Path"])

    try:
        text = extract_pdf_text(pdf_path)
        summary, embedding_text = generate_regulation_summary(text)

        row["Summary"] = summary
        row["EmbeddingText"] = embedding_text

    except Exception as e:
        logging.error(f"Failed → {pdf_path}: {e}")
        row["Summary"] = "NA"
        row["EmbeddingText"] = "NA"

    return row

def is_circular(subcategory: str) -> bool:
    if not subcategory:
        return False
    return "circular" in subcategory.lower()


def process_circular_pdf(row: pd.Series):
    pdf_path = Path(row["Path"])

    try:
        text = extract_pdf_text(pdf_path)

        # Circulars do NOT need regulation-style filtering
        core_text = text[:12000]

        summary = llm.invoke(
            CIRCULARS_PROMPT.format(text=core_text)
        ).strip()

        summary = summary.strip().strip('"')

        row["Summary"] = summary or "NA"
        row["EmbeddingText"] = core_text

    except Exception as e:
        logging.error(f"Failed → {pdf_path}: {e}")
        row["Summary"] = "NA"
        row["EmbeddingText"] = "NA"

    return row


def process_row_by_domain(row: pd.Series):
    sub = row["SubCategory"]

    if not isinstance(sub, str):
        logging.warning("SubCategory is missing or invalid")
        row["Summary"] = "NA"
        row["EmbeddingText"] = "NA"
        return row

    sub_clean = sub.strip().lower()

    if sub_clean == "regulations":
        return process_regulation_pdf(row)

    elif is_circular(sub_clean):
        return process_circular_pdf(row)

    else:
        logging.warning(f"Unsupported SubCategory: {sub}")
        row["Summary"] = "NA"
        row["EmbeddingText"] = "NA"
        return row


# ============================================================
# MAIN
# ============================================================

def main(excel_file: str):
    df = pd.read_excel(excel_file)

    required = ["Verticals", "SubCategory", "Path"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    
    logging.info(f"Processing {len(df)} PDFs across all subcategories")

    start = time.time()

    for idx, row in df.iterrows():
        logging.info(f"[{idx+1}/{len(df)}] {row['Path']}")
        processed = process_row_by_domain(row)
        update_excel(processed)

    logging.info(f"Completed in {time.time() - start:.2f}s")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    excel = DATA_DIR / "weekly_sebi_downloads.xlsx"
    if not excel.exists():
        raise FileNotFoundError("weekly_sebi_downloads.xlsx not found")

    main(excel)