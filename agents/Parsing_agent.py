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

# REGULATIONS_PROMPT = PromptTemplate(
#     template="""
# You are a senior regulatory analyst preparing a concise, client-ready summary of a SEBI regulation document
# for business stakeholders and compliance teams.

# This document establishes a regulatory framework and contains detailed legal provisions.
# The summary must allow the reader to understand what the regulation governs without opening the document.

# Focus ONLY on:
# - What the regulation governs at a high level (e.g., REITs, InvITs, intermediaries, market participants)
# - Who it applies to (e.g., sponsors, managers, trustees, listed entities, intermediaries)
# - The key compliance and governance areas covered (e.g., registration, listing, valuation, disclosures, reporting)
# - Core responsibilities imposed on regulated entities
# - The overall regulatory intent and scope (e.g., transparency, investor protection, market integrity)

# Do NOT:
# - List clauses, chapters, or definitions
# - Mention page counts, circular numbers, or legal citations
# - Explain individual amendments in detail

# Write a clear, professional, and self-contained summary.
# Limit strictly to 5–6 sentences.

# Text:
# {text}

# Final Summary (entire answer MUST be inside double quotes):
# """,
#     input_variables=["text"]
# )

# REGULATIONS_PROMPT = PromptTemplate(
#     template="""
# You are a senior regulatory analyst preparing a client-ready summary of a SEBI regulation or regulatory amendment.

# PURPOSE:
# The summary must clearly explain what SEBI has amended or introduced and what impact it has in practice.

# CONTENT RULES:
# - Focus ONLY on newly inserted or amended provisions
# - Explain the change in simple, non-legal language
# - Clearly state the impact on:
#   • investors
#   • market participants
#   • regulated entities or the public
# - Mention the practical benefit or outcome of the change
# - Avoid lengthy legal wording, clause references, or definitions
# - Do NOT mention clause numbers, insertions, omissions, substitutions, or editorial notes. If a change cannot be explained without referencing clauses, summarise its practical effect instead.

# FORMAT RULES:
# - Output ONLY bullet points
# - Use ONLY black bullet points “•”
# - Write 3–5 bullet points maximum
# - Each bullet must be ONE concise sentence

# STARTING RULE (MANDATORY):
# - The FIRST bullet MUST start with:
#   “SEBI has amended or added a regulation to …”

# TONE:
# - Professional
# - Clear
# - Suitable for business and compliance readers

# Text:
# {text}

# Final Summary:
# """,
#     input_variables=["text"]
# )

REGULATIONS_PROMPT = PromptTemplate(
    template="""
You are a senior regulatory analyst preparing a concise, client-ready summary of a SEBI regulation document
for business stakeholders and compliance teams.

This document sets out an entire regulatory framework with detailed legal provisions.
The reader will NOT open the original regulation, so the summary must explain in simple words what it covers and why it matters.

CONTENT FOCUS:
- Explain at a high level what the regulation governs (e.g., REITs, InvITs, intermediaries, market participants)
- State clearly who it applies to (e.g., sponsors, managers, trustees, listed entities, intermediaries)
- Highlight the key compliance and governance areas (e.g., registration, listing, valuation, disclosures, reporting, conduct requirements)
- Summarise the core responsibilities and obligations imposed on regulated entities
- Convey the overall regulatory intent and scope (e.g., transparency, investor protection, market integrity, better governance)

DO NOT:
- List clauses, chapters, or definitions
- Mention page counts, circular numbers, or legal citations
- Quote or paraphrase legal provisions verbatim
- Go into historical amendments or minor editorial changes

STYLE AND LENGTH:
- Use simple, non-technical language that a business reader can understand
- Keep the summary short but sufficient to give a clear picture of the framework
- Limit strictly to 5–6 sentences
- Write as a single coherent paragraph

STARTING RULE (MANDATORY):
- The first sentence MUST start with:
  “This SEBI regulation sets out the regulatory framework for …”

Text:
{text}

Final Summary (entire answer MUST be inside double quotes):
""",
    input_variables=["text"]
)

# ============================================================

# AMENDMENT_REGULATIONS_PROMPT = PromptTemplate(
#     template="""
# You are a senior regulatory analyst preparing a client-ready summary of a SEBI amendment regulation.

# The reader is a business or compliance professional who will NOT read the original document.
# The summary must clearly communicate what the amendment does in practical terms.

# STRUCTURE (MANDATORY):
# - Output ONLY bullet points (no paragraph introduction)
# - The FIRST bullet MUST start with: “The amendment”
# - The FIRST bullet should give a one-line overview of the nature of changes
# - ALL remaining bullets must describe specific changes
# - Remaining bullets MUST NOT start with headings, labels, or repeated phrases

# CONTENT RULES (STRICT):
# - Each bullet must state the outcome or impact of the change, not the legal wording
# - Do NOT use phrases like:
#   “definition of”, “the term”, “has been defined”, “has been introduced”
# - Do NOT use colon-style labels (e.g., “Change in control:”)
# - Do NOT quote or paraphrase legal definitions verbatim
# - Avoid procedural or audit mechanics unless they materially affect compliance

# FORMAT RULES (STRICT):
# - Use ONLY black bullet points “•”
# - Write a maximum of 6 bullet points
# - Each bullet must be ONE concise sentence
# - Do NOT use numbering (1., 2., 3.) or dashes (-)

# TONE:
# - Plain, professional, and client-facing
# - Written like a regulatory update shared with senior stakeholders
# - Outcome-focused and easy to scan

# Text:
# {text}

# Final Summary (use ONLY black bullet points “•”):
# """,
#     input_variables=["text"]
# )

# AMENDMENT_REGULATIONS_PROMPT = PromptTemplate(
#     template="""
# You are a senior regulatory analyst preparing a client-ready summary of a SEBI amendment regulation.

# The reader is a business or compliance professional who will NOT read the original document.
# The summary must clearly communicate what the amendment changes and why it matters in practice.

# STRUCTURE (MANDATORY):
# - Output ONLY bullet points (no paragraph introduction)
# - The FIRST bullet MUST start with: “The amendment”
# - The FIRST bullet should give a one-line overview of the nature of changes
# - ALL remaining bullets must describe specific changes and their impact

# CONTENT RULES (STRICT):
# - Each bullet must state the outcome or practical impact of the change
# - At least ONE bullet must clearly mention the impact or benefit for investors, market participants, or other stakeholders
# - Do NOT use phrases like:
#   “definition of”, “the term”, “has been defined”, “has been introduced”
# - Do NOT use colon-style labels (e.g., “Change in control:”)
# - Do NOT quote or paraphrase legal definitions verbatim
# - Avoid procedural or audit mechanics unless they materially affect compliance
# - Do NOT mention clause numbers, insertions, omissions, substitutions, or editorial notes. If a change cannot be explained without referencing clauses, summarise its practical effect instead.

# FORMAT RULES (STRICT):
# - Use ONLY black bullet points “•”
# - Write a maximum of 6 bullet points
# - Each bullet must be ONE concise sentence
# - Do NOT use numbering (1., 2., 3.) or dashes (-)

# TONE:
# - Plain, professional, and client-facing
# - Written like a regulatory update shared with senior stakeholders
# - Outcome-focused and easy to scan

# Text:
# {text}

# Final Summary (use ONLY black bullet points “•”):
# """,
#     input_variables=["text"]
# )
AMENDMENT_REGULATIONS_PROMPT = PromptTemplate(
    template="""
You are a senior regulatory analyst preparing a client-ready summary of a SEBI amendment to existing regulations.

The reader is a business or compliance professional who will NOT read the original document.
The summary must clearly explain in simple language what has changed and why it matters in practice.

STRUCTURE (MANDATORY):
- Output ONLY bullet points (no paragraph introduction)
- Use ONLY black bullet points “•”
- The FIRST bullet MUST start with: “SEBI has amended or added a regulation to …”
- The FIRST bullet must be ONE sentence that gives a high-level overview of the main changes
- ALL remaining bullets must each describe ONE important new or amended provision and its practical effect
- Do NOT use nested bullets or sub-points; every change must be its own “•” bullet

CONTENT RULES (GENERIC):
- Focus on changes that materially affect:
  • who is covered or brought into scope, or
  • what conditions, qualifications, certifications, limits or thresholds apply, or
  • what information, processes, disclosures or infrastructure are now required, or
  • how compliance, governance or investor protection will work in practice.
- Each bullet must describe the real-world outcome or impact, not the drafting mechanics
- At least ONE bullet must clearly state the impact or benefit for the public, investors or other relevant stakeholders
- Ignore minor editorial or housekeeping changes (like word substitutions, formatting, removal of fax numbers) unless they meaningfully change compliance or process
- Do NOT use phrases like:
  “definition of”, “the term”, “has been defined”, “has been introduced”
- Do NOT use colon-style labels or headings at the start of any bullet (e.g., “Impact:”, “Change:”, “Key update:”)
- Do NOT quote or paraphrase legal definitions verbatim
- Do NOT mention clause numbers, insertions, omissions, substitutions, schedules, or form item numbers; summarise their practical effect instead

FORMAT RULES (STRICT):
- Write 4 or 5 bullet points in total
- Each bullet must be ONE concise sentence
- Do NOT use numbering (1., 2., 3.) or dashes (-)
- No bullet may end with a colon

TONE:
- Plain, professional, and client-facing
- Written like a regulatory update for senior stakeholders
- Easy to scan and understand for a non-legal audience

Text:
{text}

Final Summary (use ONLY black bullet points “•”):
""",
    input_variables=["text"]
)


# CIRCULARS_PROMPT = PromptTemplate(
#     template="""
# You are a regulatory analyst writing a very short, client-ready summary of a SEBI circular.

# The output MUST strictly match the following pattern:
# - EXACTLY TWO bullet points
# - BOTH lines must be bullet points
# - The FIRST bullet MUST start with: “The Circular”

# HARD RULES (NON-NEGOTIABLE):
# - Output ONLY two bullet points, nothing else
# - Do NOT add headings, introductions, or explanations
# - If you output more or fewer bullets, the answer is INVALID

# CONTENT RULES (STRICT):
# - Summarise ONLY:
#   • the core requirement or mandate
#   • the practical compliance impact
# - Mention affected entities only if essential
# - Do NOT include:
#   • internal processes
#   • committee names
#   • reporting lines
#   • appointment mechanics
#   • legal citations, dates, or circular numbers
# - Do NOT explain background or intent explicitly

# FORMAT RULES:
# - Use ONLY black bullet points “•”
# - Each bullet must be ONE clear sentence
# - No colons, no numbering, no sub-bullets

# STYLE:
# - High-level
# - Client-facing
# - Similar in tone and length to:
#   “The Circular mandates risk disclosures at every client login.”

# Text:
# {text}

# Final Summary:
# """,
#     input_variables=["text"]
# )

# CIRCULARS_PROMPT = PromptTemplate(
#     template="""
# You are a regulatory analyst writing a short, client-ready summary of a SEBI circular.

# PURPOSE:
# SEBI circulars provide clarifications, implementation guidance, or changes to existing requirements.
# The reader should immediately understand what has changed and what it means in practice.

# CONTENT RULES:
# - Clearly state the main clarification, change, or requirement introduced by the circular
# - Mention any key condition, threshold, or timeline if stated
# - Briefly include other important informational points instead of vague phrases
# - Avoid technical or legal jargon
# - Do NOT include circular numbers, internal processes, or background explanations

# FORMAT RULES:
# - Output ONLY bullet points
# - Use ONLY black bullet points “•”
# - Write 2–4 bullet points maximum
# - Each bullet must be ONE clear sentence

# STARTING RULE (MANDATORY):
# - The FIRST bullet MUST start with:
#   “SEBI has issued this circular and …”

# TONE:
# - Plain language
# - Client-facing
# - Easy to understand for non-legal readers

# Text:
# {text}

# Final Summary:
# """,
#     input_variables=["text"]
# )

CIRCULARS_PROMPT = PromptTemplate(
    template="""
You are a regulatory analyst writing a short, client-ready summary of a SEBI circular.

SEBI circulars provide clarifications, implementation guidance, or changes to existing requirements.
The reader will NOT open the original circular, so the summary must tell them clearly what has changed and what they need to know in practice.

CONTENT RULES:
- Clearly state the main clarification, change, or requirement introduced by the circular in simple language.
- Mention any key conditions, thresholds, timelines, or applicability (for example, which entities or transactions are covered) only if they are important for compliance.
- Briefly mention other important informational points instead of using vague phrases like “other details are specified in the circular”.
- Avoid technical or legal jargon and avoid background or policy rationale unless it directly affects what must be done.
- Do NOT include circular numbers, SEBI file numbers, internal processes, committee names, venue details, or long legal citations.

FORMAT RULES:
- Output ONLY bullet points (no paragraphs or headings).
- Use ONLY black bullet points “•”.
- Write 2 to 4 bullet points in total.
- Each bullet must be ONE clear, concise sentence.
- Do NOT use numbering (1., 2., 3.) or sub-bullets.

STARTING RULE (MANDATORY):
- The FIRST bullet MUST start with:
  “SEBI has issued this circular and …”

TONE:
- Plain language and client-facing.
- Crisp, direct, and easy to understand for non-legal readers.

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