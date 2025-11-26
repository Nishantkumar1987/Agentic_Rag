
import os
import re
import uuid
import json
import pdfplumber
import camelot
from pathlib import Path
from datetime import datetime
from dateutil.tz import gettz

# ============================================================
# 1. CANONICAL CREDIT CARD STRUCTURE
# ============================================================

CANONICAL_TYPES = {
    "features": ["exclusive features", "key features", "benefits"],
    "rewards": ["rewards", "cashback", "reward points", "rewards structure"],
    "billing": ["billing", "payment", "minimum amount due", "statement"],
    "fees": ["fees", "charges", "mitc"],
    "emi_flexipay": ["flexipay", "emi"],
    "balance_transfer": ["balance transfer"],
    "insurance": ["insurance", "coverage"],
    "terms_and_conditions": ["terms", "conditions", "agreement"],
    "disputes": ["dispute", "grievance", "chargeback"]
}

# Ordered for clean flow
CANONICAL_ORDER = [
    "features", "rewards", "billing", "fees",
    "emi_flexipay", "balance_transfer",
    "insurance", "terms_and_conditions", "disputes"
]


# ============================================================
# 2. Heading Detection
# ============================================================
def detect_heading(text):
    t = text.lower().strip()

    # direct known headings
    for canon, keywords in CANONICAL_TYPES.items():
        for k in keywords:
            if t.startswith(k):
                return True

    # large uppercase lines
    if t.isupper() and len(text.split()) <= 8:
        return True

    # ends with ":" like "Rewards:"
    if text.endswith(":"):
        return True

    return False


def classify_heading(text):
    t = text.lower().strip()
    for canon, keywords in CANONICAL_TYPES.items():
        for k in keywords:
            if k in t:
                return canon
    return "unknown"


# ============================================================
# 3. Table extraction (Camelot + fallback)
# ============================================================
def extract_tables(pdf_path):
    try:
        tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
        result = []
        for t in tables:
            df = t.df
            headers = df.iloc[0].tolist()
            rows = df.iloc[1:].values.tolist()
            json_rows = [
                {headers[i]: row[i] for i in range(len(headers))}
                for row in rows
            ]
            result.append(json_rows)
        return result
    except:
        return []


# ============================================================
# 4. Main Parser
# ============================================================
def parse_creditcard_pdf(pdf_path, output_dir="parsed_creditcards"):

    product_name = Path(pdf_path).stem
    product_id = re.sub(r"[^a-zA-Z0-9]+", "_", product_name).lower()

    sections = []
    current = None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            raw = page.extract_text() or ""
            lines = raw.split("\n")

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                if detect_heading(line):
                    if current:
                        sections.append(current)

                    current = {
                        "section_id": str(uuid.uuid4()),
                        "title": line,
                        "type": classify_heading(line),
                        "text": "",
                        "tables": [],
                        "structured": {}
                    }
                else:
                    if current:
                        current["text"] += "\n" + line

        if current:
            sections.append(current)

    # attach tables
    table_groups = extract_tables(pdf_path)
    if table_groups:
        for section in sections:
            if section["type"] == "fees":
                for tbl in table_groups:
                    section["tables"].append({
                        "table_id": str(uuid.uuid4()),
                        "json": tbl
                    })

    # =====================================================
    # 5. Enrich structured fields
    # =====================================================
    for section in sections:
        text = section["text"]

        if section["type"] == "rewards":
            section["structured"] = {
                "earn_rules": re.findall(r"(\d+%|\d+\s?points?)", text),
                "exclusions": re.findall(r"not.*?on (.*)", text),
                "caps": {},
                "forfeiture_rules": "",
                "redemption": {"channels": [], "conditions": [], "expiry": ""}
            }

        if section["type"] == "billing":
            section["structured"] = {
                "billing_cycle": "",
                "payment_due_date": "",
                "min_amount_due_formula": "",
                "late_payment_fee": "",
                "allocation_order": "",
                "sma_npa_rules": ""
            }

        if section["type"] == "emi_flexipay":
            section["structured"] = {
                "tenor_options": re.findall(r"\b(\d{2}) months\b", text),
                "processing_fee": "",
                "eligibility_rules": "",
                "exclusions": []
            }

    # =====================================================
    # 6. Build final JSON
    # =====================================================
    output = {
        "product_id": product_id,
        "product_name": product_name,
        "product_line": "CreditCard",
        "issuer": "SBI Card",
        "documents": [
            {
                "file_name": Path(pdf_path).name,
                "source_type": "pdf",
                "parsed_at": datetime.now(gettz("Asia/Kolkata")).isoformat(),
                "sections": sections,
                "pages": len(sections)
            }
        ]
    }

    Path(output_dir).mkdir(exist_ok=True)
    out_path = Path(output_dir) / f"{product_id}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print("Parsed:", out_path)
    return out_path


# ============================================================
# 7. Batch Runner
# ============================================================
def parse_all_creditcards(folder_path, output_dir="parsed_json/parsed_creditcards/"):
    folder = Path(folder_path)
    for f in folder.iterdir():
        if f.suffix.lower() == ".pdf":
            print("Processing:", f.name)
            parse_creditcard_pdf(str(f), output_dir)
    print("\n✓ All credit card PDFs parsed.")
