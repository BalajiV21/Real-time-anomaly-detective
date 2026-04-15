"""
Generates HOW_TO_RUN.pdf in the project root.

    python scripts/generate_pdf.py

Requires: fpdf2  (pip install fpdf2)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fpdf import FPDF, XPos, YPos

PDF_OUT = Path(__file__).parent.parent / "HOW_TO_RUN.pdf"

C_BLACK   = (30,  30,  30)
C_WHITE   = (255, 255, 255)
C_HEADER  = (15,  55,  100)
C_ACCENT  = (30,  120, 200)
C_CODE_BG = (240, 244, 248)
C_CODE_FG = (30,  30,  80)
C_WARN_BG = (255, 248, 220)
C_STEP_BG = (232, 244, 255)
C_DIVIDER = (180, 200, 220)

MARGIN     = 15
CODE_INDENT = 18


class PDF(FPDF):
    def header(self):
        self.set_fill_color(*C_HEADER)
        self.rect(0, 0, 210, 14, "F")
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*C_WHITE)
        self.set_xy(MARGIN, 3)
        self.cell(0, 8, "Real-Time Financial Anomaly Detective  -  Run Guide", align="L")
        self.set_text_color(*C_BLACK)
        self.ln(8)

    def footer(self):
        self.set_y(-12)
        self.set_draw_color(*C_DIVIDER)
        self.line(MARGIN, self.get_y(), 210 - MARGIN, self.get_y())
        self.set_font("Helvetica", "", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 8, f"Page {self.page_no()}", align="C")
        self.set_text_color(*C_BLACK)

    def section_title(self, text: str):
        self.ln(4)
        self.set_fill_color(*C_HEADER)
        self.set_text_color(*C_WHITE)
        self.set_font("Helvetica", "B", 11)
        self.set_x(MARGIN)
        self.cell(180, 8, f"  {text}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*C_BLACK)
        self.ln(2)

    def sub_title(self, text: str):
        self.ln(3)
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*C_ACCENT)
        self.set_x(MARGIN)
        self.cell(0, 7, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*C_BLACK)

    def body(self, text: str):
        self.set_font("Helvetica", "", 9)
        self.set_x(MARGIN)
        self.multi_cell(180, 5, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def code_block(self, lines: list):
        self.set_fill_color(*C_CODE_BG)
        self.set_draw_color(*C_DIVIDER)
        total_h = len(lines) * 5 + 4
        x = CODE_INDENT
        y = self.get_y() + 1
        self.rect(x, y, 180 - CODE_INDENT + MARGIN, total_h, "FD")
        self.set_xy(x + 3, y + 2)
        self.set_font("Courier", "", 8)
        self.set_text_color(*C_CODE_FG)
        for line in lines:
            self.set_x(x + 3)
            self.cell(0, 5, line, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(*C_BLACK)
        self.set_font("Helvetica", "", 9)
        self.ln(2)

    def note_box(self, text: str, icon: str = "NOTE"):
        self.set_fill_color(*C_WARN_BG)
        self.set_draw_color(200, 160, 0)
        self.set_line_width(0.4)
        x = MARGIN
        y = self.get_y() + 1
        self.set_xy(x, y)
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(140, 90, 0)
        self.cell(180, 5, f"  {icon}", fill=True, border="L", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_x(x)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(*C_BLACK)
        self.set_fill_color(*C_WARN_BG)
        self.multi_cell(180, 5, f"  {text}", fill=True, border="LB", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_line_width(0.2)
        self.ln(2)

    def table(self, headers: list, rows: list, col_widths: list):
        self.set_fill_color(*C_HEADER)
        self.set_text_color(*C_WHITE)
        self.set_font("Helvetica", "B", 8)
        self.set_x(MARGIN)
        for h, w in zip(headers, col_widths):
            self.cell(w, 6, f" {h}", fill=True, border=1)
        self.ln()
        self.set_text_color(*C_BLACK)
        for i, row in enumerate(rows):
            bg = (248, 252, 255) if i % 2 == 0 else C_WHITE
            self.set_fill_color(*bg)
            self.set_font("Helvetica", "", 8)
            self.set_x(MARGIN)
            for cell, w in zip(row, col_widths):
                self.cell(w, 5, f" {cell}", fill=True, border=1)
            self.ln()
        self.ln(2)


pdf = PDF()
pdf.set_auto_page_break(auto=True, margin=16)
pdf.set_margins(MARGIN, 18, MARGIN)
pdf.add_page()

# Cover
pdf.set_fill_color(*C_HEADER)
pdf.rect(0, 14, 210, 60, "F")
pdf.set_font("Helvetica", "B", 22)
pdf.set_text_color(*C_WHITE)
pdf.set_xy(MARGIN, 28)
pdf.cell(0, 12, "Real-Time Financial Anomaly Detective", align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
pdf.set_font("Helvetica", "", 11)
pdf.set_text_color(180, 210, 255)
pdf.set_xy(MARGIN, 48)
pdf.cell(0, 7, "How-To-Run Guide", align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
pdf.set_text_color(*C_BLACK)
pdf.set_xy(MARGIN, 78)

pdf.section_title("Section 1  -  Prerequisites")
pdf.body("1. Install Docker Desktop (docker.com) and ensure it is running.")
pdf.body("2. Get a free Finnhub API key at finnhub.io/register.")
pdf.body("3. Install Java JDK 17 (adoptium.net) and set JAVA_HOME.")
pdf.body("4. Install Python dependencies:")
pdf.code_block(["pip install -r requirements.txt"])
pdf.body("5. Create .env from .env.example and add FINNHUB_API_KEY.")

pdf.add_page()
pdf.section_title("Section 2  -  Running the Pipeline")
pdf.sub_title("Start infrastructure")
pdf.code_block(["docker-compose up -d", "# Verify all containers are healthy:", "docker-compose ps"])
pdf.sub_title("Initialise database schema (once)")
pdf.code_block(["python -c \"from storage.connection import init_db; init_db()\""])
pdf.sub_title("Start all components (one terminal each)")
pdf.code_block([
    "python -m ingestion.trade_producer",
    "python -m ingestion.quote_producer",
    "python -m ingestion.news_producer",
    "python -m ingestion.sentiment_producer",
    "python -m streaming.consumer",
    "streamlit run dashboard/app.py",
])
pdf.note_box("Dashboard: http://localhost:8501   Kafka UI: http://localhost:8080", icon="URLS")

pdf.add_page()
pdf.section_title("Section 3  -  Detection Methods")
pdf.table(
    headers=["Method", "Type", "Trigger"],
    rows=[
        ["Z-Score",          "Statistical",     "|z| > 3.0"],
        ["IQR Fence",        "Statistical",     "Outside Q1-1.5*IQR ... Q3+1.5*IQR"],
        ["Volume Spike",     "Threshold",       "Volume > 5x rolling average"],
        ["Isolation Forest", "ML",              "Score > 0.55 (after training)"],
    ],
    col_widths=[42, 36, 102],
)
pdf.sub_title("Train the ML model (after 30+ min of data)")
pdf.code_block([
    "python -c \"from detection.ml import IsolationForestDetector; \\",
    "           IsolationForestDetector().train_from_db()\"",
])

pdf.section_title("Section 4  -  Troubleshooting")
pdf.table(
    headers=["Problem", "Fix"],
    rows=[
        ["Containers not healthy",      "docker-compose restart"],
        ["init_db() connection refused","DB still starting  -  wait 10s and retry"],
        ["No WebSocket trades",         "Market closed  -  BINANCE:BTCUSDT works 24/7"],
        ["Spark JAVA_HOME error",       "Install JDK 17 and set JAVA_HOME env var"],
        ["Dashboard DB connection error","Ensure docker-compose up -d ran first"],
    ],
    col_widths=[75, 105],
)

pdf.output(str(PDF_OUT))
print(f"PDF written to {PDF_OUT}")
