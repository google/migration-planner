# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.pdfgen import canvas


class NumberedCanvas(canvas.Canvas):
    """Custom canvas to draw running headers, footers and page numbers on all pages."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_decorations(num_pages)
            super().showPage()
        super().save()

    def draw_page_decorations(self, page_count):
        self.saveState()
        self.setFont("Helvetica-Bold", 8)
        self.setFillColor(colors.HexColor("#1E3A8A"))
        
        # Running Header
        self.drawString(54, 750, "M365 TENANT ASSESSMENT")
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#64748B"))
        self.drawRightString(558, 750, "Executive Summary & Action Plan")
        
        # Header Line
        self.setStrokeColor(colors.HexColor("#E2E8F0"))
        self.setLineWidth(0.5)
        self.line(54, 742, 558, 742)
        
        # Footer Line
        self.line(54, 52, 558, 52)
        
        # Running Footer
        self.drawString(54, 40, "Confidential - Tenant Executive Summary Sheet")
        page_text = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(558, 40, page_text)
        
        self.restoreState()


def get_severity_color(severity: str) -> colors.Color:
    """Returns color associated with risk severity."""
    sev = severity.strip().lower()
    if sev == "high":
        return colors.HexColor("#DC2626")  # Red
    elif sev == "medium":
        return colors.HexColor("#D97706")  # Orange
    return colors.HexColor("#4B5563")      # Dark Grey/Slate


def generate_pdf_summary_report(summary_data: dict, filepath: str, tenant_id: str = "N/A"):
    """Compiles the Gemini summary JSON data into a clean, compact 1-2 page PDF document."""
    
    # Page setup
    doc = SimpleDocTemplate(
        filepath,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=68,
        bottomMargin=68
    )
    
    styles = getSampleStyleSheet()
    
    # Custom colors
    primary_color = colors.HexColor("#1E3A8A")   # Navy Accent
    secondary_color = colors.HexColor("#475569") # Slate Secondary
    text_color = colors.HexColor("#1E293B")      # Charcoal Body Text
    outline_color = colors.HexColor("#E2E8F0")   # Border light grey
    
    # Custom styles
    styles['Normal'].textColor = text_color
    styles['Normal'].fontSize = 9
    styles['Normal'].leading = 13
    
    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=18,
        leading=22,
        textColor=primary_color,
        spaceAfter=3
    )
    
    subtitle_style = ParagraphStyle(
        'DocSubtitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        leading=13,
        textColor=secondary_color,
        spaceAfter=10
    )
    
    meta_label_style = ParagraphStyle(
        'MetaLabel',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=8.5,
        textColor=secondary_color
    )
    
    meta_val_style = ParagraphStyle(
        'MetaValue',
        parent=styles['Normal'],
        fontSize=8.5,
        textColor=text_color
    )
    
    h1_style = ParagraphStyle(
        'SectionH1',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=12,
        leading=15,
        textColor=primary_color,
        spaceBefore=12,
        spaceAfter=6,
        keepWithNext=True
    )
    
    body_style = ParagraphStyle(
        'SummaryBody',
        parent=styles['Normal'],
        fontSize=9,
        leading=13,
        spaceAfter=6
    )
    
    table_cell_style = ParagraphStyle(
        'TableCell',
        parent=styles['Normal'],
        fontSize=8,
        leading=11
    )
    
    table_cell_bold = ParagraphStyle(
        'TableCellBold',
        parent=table_cell_style,
        fontName='Helvetica-Bold',
        textColor=primary_color
    )
    
    table_cell_header = ParagraphStyle(
        'TableCellHeader',
        parent=table_cell_style,
        fontName='Helvetica-Bold',
        textColor=colors.white
    )

    story = []

    # =========================================================================
    # HEADER BANNER & METADATA
    # =========================================================================
    story.append(Paragraph(summary_data.get("title", "EXECUTIVE SUMMARY: MICROSOFT 365 TENANT ASSESSMENT"), title_style))
    story.append(Paragraph(summary_data.get("subtitle", "Strategic Insights and Recommendations for Tenant Optimization"), subtitle_style))
    
    # Compact Metadata Table
    gen_time_str = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    meta_data = [
        [
            Paragraph("Tenant ID:", meta_label_style), Paragraph(tenant_id, meta_val_style),
            Paragraph("Generated on:", meta_label_style), Paragraph(gen_time_str, meta_val_style)
        ]
    ]
    meta_table = Table(meta_data, colWidths=[60, 190, 80, 174])
    meta_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('LINEBELOW', (0, 0), (-1, -1), 0.5, outline_color),
    ]))
    story.append(meta_table)
    story.append(Spacer(1, 10))

    # =========================================================================
    # SECTION 1: OVERVIEW
    # =========================================================================
    story.append(Paragraph("Overview", h1_style))
    overview_text = summary_data.get("overview", "")
    story.append(Paragraph(overview_text, body_style))
    story.append(Spacer(1, 5))

    # =========================================================================
    # SECTION 2: KEY STRATEGIC METRICS
    # =========================================================================
    story.append(Paragraph("Key Strategic Metrics", h1_style))
    key_metrics = summary_data.get("key_metrics", [])
    if not key_metrics:
        story.append(Paragraph("No strategic metrics specified.", body_style))
    else:
        metric_table_data = [[
            Paragraph("Metric Indicator", table_cell_header),
            Paragraph("Measurement", table_cell_header),
            Paragraph("Analysis & Scope Context", table_cell_header)
        ]]
        for metric in key_metrics:
            metric_table_data.append([
                Paragraph(metric.get("label", "N/A"), table_cell_bold),
                Paragraph(metric.get("value", "N/A"), table_cell_bold),
                Paragraph(metric.get("detail", "N/A"), table_cell_style)
            ])
        
        metric_table = Table(metric_table_data, colWidths=[160, 90, 254])
        metric_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(metric_table)
    story.append(Spacer(1, 5))

    # =========================================================================
    # SECTION 3: CRITICAL FINDINGS & RISKS
    # =========================================================================
    story.append(Paragraph("Critical Findings & Risk Assessment", h1_style))
    findings = summary_data.get("critical_findings", [])
    if not findings:
        story.append(Paragraph("No critical findings or risks identified.", body_style))
    else:
        findings_table_data = [[
            Paragraph("Category", table_cell_header),
            Paragraph("Severity", table_cell_header),
            Paragraph("Finding Summary Details", table_cell_header)
        ]]
        for finding in findings:
            sev_text = finding.get("severity", "medium").upper()
            sev_color = get_severity_color(sev_text)
            sev_cell_style = ParagraphStyle(
                'SevCell',
                parent=table_cell_style,
                fontName='Helvetica-Bold',
                textColor=sev_color
            )
            findings_table_data.append([
                Paragraph(finding.get("category", "N/A"), table_cell_bold),
                Paragraph(sev_text, sev_cell_style),
                Paragraph(finding.get("finding", "N/A"), table_cell_style)
            ])
            
        findings_table = Table(findings_table_data, colWidths=[100, 60, 344])
        findings_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(findings_table)
    story.append(Spacer(1, 5))

    # =========================================================================
    # SECTION 4: STRATEGIC RECOMMENDATIONS
    # =========================================================================
    story.append(Paragraph("Strategic Recommendations & Action Plan", h1_style))
    recs = summary_data.get("strategic_recommendations", [])
    if not recs:
        story.append(Paragraph("No strategic recommendations provided.", body_style))
    else:
        recs_table_data = [[
            Paragraph("Category & Focus", table_cell_header),
            Paragraph("Priority", table_cell_header),
            Paragraph("Action Item Recommendation", table_cell_header)
        ]]
        for rec in recs:
            prio_text = rec.get("priority", "medium").upper()
            prio_color = get_severity_color(prio_text)
            prio_cell_style = ParagraphStyle(
                'PrioCell',
                parent=table_cell_style,
                fontName='Helvetica-Bold',
                textColor=prio_color
            )
            rec_desc = f"<b>{rec.get('title', 'N/A')}</b><br/>{rec.get('description', 'N/A')}"
            recs_table_data.append([
                Paragraph(rec.get("category", "N/A"), table_cell_bold),
                Paragraph(prio_text, prio_cell_style),
                Paragraph(rec_desc, table_cell_style)
            ])
            
        recs_table = Table(recs_table_data, colWidths=[120, 60, 324])
        recs_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(recs_table)

    # Build document using dynamic page counting NumberedCanvas
    doc.build(story, canvasmaker=NumberedCanvas)
