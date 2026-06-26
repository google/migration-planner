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

"""PDF Report Compilation module for Microsoft 365 Tenant Telemetry data."""

import io
from datetime import datetime
from collections import Counter
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.pdfgen import canvas


class NumberedCanvas(canvas.Canvas):
    """Custom canvas to compute total page count and draw running headers, footers and page numbers."""
    
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
        if self._pageNumber == 1:
            # Skip page number and decorations on the cover page
            return
            
        self.saveState()
        self.setFont("Helvetica-Bold", 8)
        self.setFillColor(colors.HexColor("#1E3A8A"))
        
        # Header
        self.drawString(54, 750, "DEAL ASSISTANT")
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#64748B"))
        self.drawRightString(558, 750, "M365 Tenant Telemetry & Audit Report")
        
        # Header Line
        self.setStrokeColor(colors.HexColor("#E2E8F0"))
        self.setLineWidth(0.5)
        self.line(54, 742, 558, 742)
        
        # Footer Line
        self.line(54, 52, 558, 52)
        
        # Footer
        self.drawString(54, 40, "Confidential - Tenant Audit Assessment")
        page_text = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(558, 40, page_text)
        
        self.restoreState()


def format_prepaid_units(item: dict) -> str:
    prepaid = item.get("prepaidUnits", {})
    p_str = f"Enabled: {prepaid.get('enabled', 0):,}"
    if prepaid.get('warning', 0) > 0:
        p_str += f"\nWarn: {prepaid.get('warning'):,}"
    if prepaid.get('suspended', 0) > 0:
        p_str += f"\nSusp: {prepaid.get('suspended'):,}"
    return p_str


def generate_trend_chart_bytes(trend_data: dict) -> io.BytesIO:
    """Generates the O365 Active User Trend Chart on-the-fly to minimize persistent memory footprint."""
    
    dates = trend_data.get("dates", [])
    if not dates:
        return None
        
    fig = Figure(figsize=(6.5, 3.2), dpi=150)
    ax = fig.add_subplot(111)
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#FFFFFF")
    
    # Palette tailored to Match Deal Assistant theme
    ax.plot(dates, trend_data.get("office365", []), marker='o', markersize=3, linewidth=1.5, label='Office 365', color="#1E3A8A")
    ax.plot(dates, trend_data.get("exchange", []), marker='o', markersize=3, linewidth=1.5, label='Exchange', color="#C2410C")
    ax.plot(dates, trend_data.get("onedrive", []), marker='o', markersize=3, linewidth=1.5, label='OneDrive', color="#3B82F6")
    ax.plot(dates, trend_data.get("sharepoint", []), marker='o', markersize=3, linewidth=1.5, label='SharePoint', color="#15803D")
    ax.plot(dates, trend_data.get("teams", []), marker='o', markersize=3, linewidth=1.5, label='Teams', color="#9333EA")
    
    ax.set_xlabel("Date", fontsize=8, color="#475569")
    ax.set_ylabel("Active Users", fontsize=8, color="#475569")
    ax.tick_params(axis='x', colors="#475569", rotation=45, labelsize=7)
    ax.tick_params(axis='y', colors="#475569", labelsize=7)
    
    if len(dates) > 10:
        ax.set_xticks(dates[::max(1, len(dates)//10)])
        
    for spine in ax.spines.values():
        spine.set_color("#CBD5E1")
        
    ax.legend(facecolor="#FFFFFF", edgecolor="#CBD5E1", labelcolor="#1E293B", fontsize=8)
    fig.tight_layout()
    
    buf = io.BytesIO()
    canvas = FigureCanvasAgg(fig)
    canvas.print_png(buf)
    buf.seek(0)
    return buf


def generate_pa_chart_bytes(pa: dict) -> io.BytesIO:
    """Generates the Power Automate Flows breakdown bar chart on-the-fly."""
    
    counts = pa.get("counts", {})
    if not counts:
        return None
        
    active_counts = pa.get("active_counts", {})
    tier_counts = pa.get("tier_counts", {})
    active_tier_counts = pa.get("active_tier_counts", {})
    complex_flows = pa.get("complex_logic_flows", [])
    
    fig = Figure(figsize=(6.5, 3.2), dpi=150)
    ax = fig.add_subplot(111)
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#FFFFFF")
    
    categories = ['Cloud Flows', 'Desktop Flows', 'Personal', 'Enterprise', 'Complex']
    
    c_total = counts.get("Cloud Flows", 0)
    c_active = active_counts.get("Cloud Flows", 0)
    c_inactive = c_total - c_active
    
    d_total = counts.get("Desktop Flows", 0)
    d_active = active_counts.get("Desktop Flows", 0)
    d_inactive = d_total - d_active
    
    p_total = tier_counts.get("Personal Productivity", 0)
    p_active = active_tier_counts.get("Personal Productivity", 0)
    p_inactive = p_total - p_active
    
    e_total = tier_counts.get("Enterprise/Departmental", 0)
    e_active = active_tier_counts.get("Enterprise/Departmental", 0)
    e_inactive = e_total - e_active
    
    complex_active = sum(1 for f in complex_flows if f.get("Active") == "Yes")
    complex_inactive = len(complex_flows) - complex_active
    
    actives = [c_active, d_active, p_active, e_active, complex_active]
    inactives = [c_inactive, d_inactive, p_inactive, e_inactive, complex_inactive]
    
    x = range(len(categories))
    width = 0.25
    
    rects1 = ax.bar(x, actives, width, label='Active', color="#1E3A8A")
    rects2 = ax.bar([i + width for i in x], inactives, width, label='Inactive', color="#CBD5E1")
    
    ax.set_ylabel('Count', color="#1E293B", fontsize=8, fontweight='bold')
    ax.set_title('Power Automate Flows Breakdown', color="#1E293B", fontsize=9, fontweight='bold')
    ax.set_xticks([i + width/2 for i in x])
    ax.set_xticklabels(categories, color="#1E293B", fontsize=8, fontweight='bold')
    ax.legend(facecolor="#FFFFFF", edgecolor="#CBD5E1", labelcolor="#1E293B", prop={'size':8})
    
    ax.bar_label(rects1, padding=2, color="#1E293B", fontsize=7)
    ax.bar_label(rects2, padding=2, color="#1E293B", fontsize=7)
    
    for spine in ax.spines.values():
        spine.set_color("#CBD5E1")
        
    ax.tick_params(axis='y', colors="#1E293B", labelsize=8)
    
    max_val = max(max(actives), max(inactives))
    ax.set_ylim(0, max(max_val + 3, int(max_val * 1.3)))
    
    fig.tight_layout()
    buf = io.BytesIO()
    canvas = FigureCanvasAgg(fig)
    canvas.print_png(buf)
    buf.seek(0)
    return buf


def generate_pdf_report(data: dict, filepath: str):
    """Generates a beautifully structured PDF document summarizing all tenant telemetry statistics."""
    
    # 1. Document Setup
    # 54pt margins correspond to 0.75 inches
    doc = SimpleDocTemplate(
        filepath,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=64,
        bottomMargin=64
    )
    
    styles = getSampleStyleSheet()
    
    # Custom color palette
    primary_color = colors.HexColor("#1E3A8A")   # Navy Accent
    secondary_color = colors.HexColor("#475569") # Slate Secondary
    text_color = colors.HexColor("#1E293B")      # Charcoal Body Text
    outline_color = colors.HexColor("#CBD5E1")   # Border light grey
    
    # Modify default styles in-place
    styles['Normal'].textColor = text_color
    styles['Normal'].fontSize = 9
    styles['Normal'].leading = 13
    
    # Custom styles
    title_style = ParagraphStyle(
        'CoverTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=26,
        leading=32,
        textColor=primary_color,
        spaceAfter=10
    )
    
    subtitle_style = ParagraphStyle(
        'CoverSubtitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=13,
        leading=18,
        textColor=secondary_color,
        spaceAfter=30
    )
    
    h1_style = ParagraphStyle(
        'SectionH1',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=15,
        leading=18,
        textColor=primary_color,
        spaceBefore=22,
        spaceAfter=10,
        keepWithNext=True
    )
    
    h2_style = ParagraphStyle(
        'SectionH2',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        leading=14,
        textColor=secondary_color,
        spaceBefore=14,
        spaceAfter=6,
        keepWithNext=True
    )
    
    body_style = ParagraphStyle(
        'ReportBody',
        parent=styles['Normal'],
        fontSize=9,
        leading=13,
        spaceAfter=6
    )
    
    bold_body_style = ParagraphStyle(
        'ReportBodyBold',
        parent=body_style,
        fontName='Helvetica-Bold'
    )
    
    table_cell_style = ParagraphStyle(
        'TableCell',
        parent=styles['Normal'],
        fontSize=8.5,
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

    small_table_cell_style = ParagraphStyle(
        'SmallTableCell',
        parent=styles['Normal'],
        fontSize=6.0,
        leading=7.5
    )
    
    small_table_cell_bold = ParagraphStyle(
        'SmallTableCellBold',
        parent=small_table_cell_style,
        fontName='Helvetica-Bold',
        textColor=primary_color
    )
    
    small_table_cell_header = ParagraphStyle(
        'SmallTableCellHeader',
        parent=small_table_cell_style,
        fontName='Helvetica-Bold',
        textColor=colors.white
    )
    
    meta_label_style = ParagraphStyle(
        'MetaLabel',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=10,
        textColor=secondary_color
    )
    
    meta_val_style = ParagraphStyle(
        'MetaValue',
        parent=styles['Normal'],
        fontSize=10,
        textColor=text_color
    )

    story = []

    # =========================================================================
    # COVER PAGE
    # =========================================================================
    story.append(Spacer(1, 120))
    story.append(Paragraph("🤝 Deal Assistant", ParagraphStyle('Branding', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=18, textColor=primary_color, spaceAfter=20)))
    story.append(Paragraph("Microsoft 365 Tenant<br/>Audit & Telemetry Report", title_style))
    story.append(Paragraph("A comprehensive assessment of license allocations, workload adoption patterns, security configurations, and workflow automation.", subtitle_style))
    story.append(Spacer(1, 100))
    
    # Metadata Table
    meta_data = [
        [Paragraph("Tenant Name / ID:", meta_label_style), Paragraph(data.get("tenant_id", "N/A"), meta_val_style)],
        [Paragraph("Report Generated:", meta_label_style), Paragraph(datetime.now().strftime("%B %d, %Y at %I:%M %p"), meta_val_style)],
        [Paragraph("Assessment Status:", meta_label_style), Paragraph("🟢 Audit Completed Successfully", ParagraphStyle('StatusStyle', parent=meta_val_style, fontName='Helvetica-Bold', textColor=colors.HexColor("#15803D")))],
        [Paragraph("Report Context:", meta_label_style), Paragraph("Usage & Adoption Inventory", meta_val_style)]
    ]
    
    meta_table = Table(meta_data, colWidths=[130, 370])
    meta_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('LINEBELOW', (0, 0), (-1, -2), 0.5, colors.HexColor("#F1F5F9")),
    ]))
    story.append(meta_table)
    story.append(PageBreak())

    # =========================================================================
    # SECTION 1: SUBSCRIBED SKUS INVENTORY
    # =========================================================================
    story.append(Paragraph("1. Subscribed SKUs", h1_style))
    story.append(Paragraph("This section outlines the licensing packages (SKUs) currently configured and active in your Microsoft Entra ID tenant scope, displaying total enabled vs. consumed license counts.", body_style))
    story.append(Spacer(1, 8))
    
    sku_list = data.get("skus", [])
    if not sku_list:
        story.append(Paragraph("No subscribed licensing data was discovered or available for this report.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        # Table columns: SKU, Units, Consumed
        sku_table_data = [[
            Paragraph("SKU Part Number", table_cell_header),
            Paragraph("Allocated Units Status", table_cell_header),
            Paragraph("Consumed Units", table_cell_header)
        ]]
        
        for item in sku_list:
            sku_table_data.append([
                Paragraph(item.get("skuPartNumber", "UNKNOWN_SKU"), table_cell_bold),
                Paragraph(format_prepaid_units(item).replace("\n", "<br/>"), table_cell_style),
                Paragraph(f"{item.get('consumedUnits', 0):,}", table_cell_style)
            ])
            
        sku_table = Table(sku_table_data, colWidths=[220, 160, 120])
        sku_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(sku_table)
    story.append(Spacer(1, 15))

    # =========================================================================
    # SECTION 1b: DIRECTORY SUMMARY
    # =========================================================================
    story.append(Paragraph("1b. Directory Summary", h1_style))

    dir_data = data.get("directory", {})
    if not dir_data:
        story.append(Paragraph("No directory telemetry data was available.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        # 1. Organization Details Table
        story.append(Paragraph("Organization Details", h2_style))
        story.append(Paragraph("This section outlines general configuration parameters, tenant types, sync properties, and active services/plans configured for the tenant organization.", body_style))
        story.append(Spacer(1, 8))
        
        org_list = dir_data.get("organization", [])
        if not org_list:
            story.append(Paragraph("No organization configuration details were available.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
        else:
            org = org_list[0] if org_list else {}
            plans = org.get("provisionedPlans", [])
            plan_services = sorted(list(set(plan.get("service") for plan in plans if plan.get("service"))))
            plan_services_str = ", ".join(plan_services) if plan_services else "null"
            
            def format_pdf_val(v):
                return "null" if v is None else str(v)
                
            org_table_data = [
                [Paragraph("Property", table_cell_header), Paragraph("Value", table_cell_header)],
                [Paragraph("displayName", table_cell_bold), Paragraph(format_pdf_val(org.get("displayName")), table_cell_style)],
                [Paragraph("isMultipleDataLocationsForServicesEnabled", table_cell_bold), Paragraph(format_pdf_val(org.get("isMultipleDataLocationsForServicesEnabled")), table_cell_style)],
                [Paragraph("onPremisesSyncEnabled", table_cell_bold), Paragraph(format_pdf_val(org.get("onPremisesSyncEnabled")), table_cell_style)],
                [Paragraph("onPremisesLastSyncDateTime", table_cell_bold), Paragraph(format_pdf_val(org.get("onPremisesLastSyncDateTime")), table_cell_style)],
                [Paragraph("partnerTenantType", table_cell_bold), Paragraph(format_pdf_val(org.get("partnerTenantType")), table_cell_style)],
                [Paragraph("tenantType", table_cell_bold), Paragraph(format_pdf_val(org.get("tenantType")), table_cell_style)],
                [Paragraph("provisionedPlans", table_cell_bold), Paragraph(plan_services_str, table_cell_style)]
            ]
            
            org_table = Table(org_table_data, colWidths=[200, 300])
            org_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(org_table)
            story.append(Spacer(1, 4))
            story.append(Paragraph("<font size=8 color='#6B7280'>* If OnPremisesSyncEnabled returns True, on-premises Active Directory is a primary source of truth. If it returns Null or False, the directory is cloud-managed or driven by a 3rd-party application.</font>", body_style))
            
        story.append(Spacer(1, 15))

        # 2. Domains Table
        story.append(Paragraph("Domains", h2_style))
        story.append(Paragraph("This section displays the configured internet domains associated with the tenant and their verified statuses.", body_style))
        story.append(Spacer(1, 8))
        
        domains = dir_data.get("domains", [])
        if not domains:
            story.append(Paragraph("No domains discovered in directory scope.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
        else:
            domains_table_data = [[
                Paragraph("Domain ID", table_cell_header),
                Paragraph("Auth Type", table_cell_header),
                Paragraph("Admin Managed", table_cell_header),
                Paragraph("Default", table_cell_header),
                Paragraph("Verified", table_cell_header),
                Paragraph("Supported Services", table_cell_header),
                Paragraph("Federation Display Name", table_cell_header),
                Paragraph("Federation Issuer URI", table_cell_header)
            ]]
            for item in domains:
                auth_type = item.get("authenticationType", "N/A") or "N/A"
                admin_managed = "Yes" if item.get("isAdminManaged") else "No"
                is_default = "Yes" if item.get("isDefault") else "No"
                is_verified = "Yes" if item.get("isVerified") else "No"
                services = item.get("supportedServices", [])
                services_str = ", ".join(services) if services else "-"
                fed_idp = item.get("federationDisplayName") or "-"
                fed_issuer = item.get("federationIssuerUri") or "-"
                
                domains_table_data.append([
                    Paragraph(item.get("id", "-"), table_cell_bold),
                    Paragraph(auth_type, table_cell_style),
                    Paragraph(admin_managed, table_cell_style),
                    Paragraph(is_default, table_cell_style),
                    Paragraph(is_verified, table_cell_style),
                    Paragraph(services_str, table_cell_style),
                    Paragraph(fed_idp, table_cell_style),
                    Paragraph(fed_issuer, table_cell_style)
                ])
            domains_table = Table(domains_table_data, colWidths=[80, 50, 45, 35, 35, 95, 80, 80])
            domains_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(domains_table)
            story.append(Spacer(1, 4))
            story.append(Paragraph("<font size=8 color='#6B7280'>* AuthenticationType=Managed indicates a cloud managed domain where Microsoft Entra ID performs user authentication. Federated indicates authentication is federated with an identity provider (eg. AD FS, Okta etc.)</font>", body_style))
            
        story.append(Spacer(1, 15))

        # 2b. User Creation/Deletion Logs Table
        story.append(Paragraph("User Creation/Deletion Logs", h2_style))
        story.append(Paragraph("This section displays directory audit logs for user creation and deletion events, indicating who initiated the action and the associated details.", body_style))
        story.append(Spacer(1, 8))
        
        user_creation_logs = dir_data.get("user_creation_logs", [])
        if not user_creation_logs:
            story.append(Paragraph("No user creation or deletion audit logs discovered.", body_style))
        elif user_creation_logs[0].get("activity") == "ERROR":
            err_msg = user_creation_logs[0].get("initiatedBy")
            story.append(Paragraph(f"Error: {err_msg}", ParagraphStyle('ErrTxtUserCreation', parent=body_style, textColor=colors.HexColor("#DC2626"))))
        else:
            user_creation_table_data = [[
                Paragraph("Activity", table_cell_header),
                Paragraph("Initiated By", table_cell_header)
            ]]
            
            for log in user_creation_logs:
                activity = log.get("activity") or "-"
                init_by = log.get("initiatedBy") or "-"
                
                user_creation_table_data.append([
                    Paragraph(activity, table_cell_bold),
                    Paragraph(init_by, table_cell_style)
                ])
                
            user_creation_table = Table(user_creation_table_data, colWidths=[124, 380])
            user_creation_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(user_creation_table)
            story.append(Spacer(1, 4))
            story.append(Paragraph("<font size=8 color='#6B7280'>* Based on sampled data collected from audit logs.</font>", body_style))
        
        story.append(Spacer(1, 15))

        # 2c. Provisioning Logs Table
        story.append(Paragraph("Provisioning Logs", h2_style))
        story.append(Paragraph("This section displays directory provisioning audit logs, indicating identity synchronization actions, status info, and target details.", body_style))
        story.append(Spacer(1, 8))
        
        provisioning_logs = dir_data.get("provisioning_logs", [])
        if not provisioning_logs:
            story.append(Paragraph("No provisioning audit logs discovered.", body_style))
        elif provisioning_logs[0].get("initiatedBy") == "ERROR":
            err_msg = provisioning_logs[0].get("provisioningAction")
            story.append(Paragraph(f"Error: {err_msg}", ParagraphStyle('ErrTxtProvisioning', parent=body_style, textColor=colors.HexColor("#DC2626"))))
        else:
            prov_table_data = [[
                Paragraph("Initiated By", small_table_cell_header),
                Paragraph("Action", small_table_cell_header),
                Paragraph("Steps", small_table_cell_header),
                Paragraph("Service Principal", small_table_cell_header),
                Paragraph("Source System", small_table_cell_header),
                Paragraph("Target System", small_table_cell_header),
                Paragraph("Tenant ID", small_table_cell_header),
                Paragraph("Status Info", small_table_cell_header)
            ]]
            
            for log in provisioning_logs:
                initiatedBy = log.get("initiatedBy") or "-"
                action = log.get("provisioningAction") or "-"
                steps = log.get("provisioningSteps") or "-"
                sp = log.get("servicePrincipal") or "-"
                src = log.get("sourceSystem") or "-"
                tgt = log.get("targetSystem") or "-"
                tenant = log.get("tenantId") or "-"
                statusInfo = log.get("provisioningStatusInfo") or "-"
                
                prov_table_data.append([
                    Paragraph(initiatedBy, small_table_cell_style),
                    Paragraph(action, small_table_cell_bold),
                    Paragraph(steps, small_table_cell_style),
                    Paragraph(sp, small_table_cell_style),
                    Paragraph(src, small_table_cell_style),
                    Paragraph(tgt, small_table_cell_style),
                    Paragraph(tenant, small_table_cell_style),
                    Paragraph(statusInfo, small_table_cell_style)
                ])
                
            prov_table = Table(prov_table_data, colWidths=[64, 60, 80, 60, 40, 40, 40, 120])
            prov_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('LEFTPADDING', (0, 0), (-1, -1), 3),
                ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(prov_table)
            story.append(Spacer(1, 4))
            story.append(Paragraph("<font size=8 color='#6B7280'>* Based on sampled data collected from audit logs.</font>", body_style))
        
        story.append(Spacer(1, 15))
        
        # 3. Groups & Users Table
        story.append(Paragraph("Groups & Users", h2_style))
        story.append(Paragraph("This section displays counts of different directory user and group categories configured in Microsoft Entra ID.", body_style))
        story.append(Spacer(1, 8))
        
        group_counts = dir_data.get("group_counts", {})
        user_counts = dir_data.get("user_counts", {})
        
        dir_table_data = [[
            Paragraph("Category", table_cell_header),
            Paragraph("Count", table_cell_header)
        ]]
        
        rows_spec = [
            # User statistics
            ("Total Users", user_counts.get("total", 0), True),
            ("Enabled Users", user_counts.get("enabled", 0), False),
            ("Disabled Users", user_counts.get("disabled", 0), False),
            ("Member Users", user_counts.get("member", 0), False),
            ("Guest Users", user_counts.get("guest", 0), False),
            # Spacing placeholder
            ("", "", False),
            # Group statistics
            ("Total Groups", group_counts.get("total", 0), True),
            ("Microsoft 365 Groups (Unified)", group_counts.get("m365", 0), False),
            ("Security Groups (Static, non-mail-enabled)", group_counts.get("security", 0), False),
            ("Mail-enabled Security Groups", group_counts.get("mail_enabled_security", 0), False),
            ("Distribution Groups", group_counts.get("distribution", 0), False),
            ("Dynamic Groups (Dynamic Membership)", group_counts.get("dynamic", 0), False)
        ]

        row_backgrounds = []
        for idx, item in enumerate(rows_spec, start=1):
            metric_name, val, is_bold = item
            if metric_name == "":
                dir_table_data.append([Paragraph("", table_cell_style), Paragraph("", table_cell_style)])
                # Divider background color
                row_backgrounds.append((idx, colors.HexColor("#CBD5E1")))
                continue
                
            cell_bold = table_cell_bold if is_bold else table_cell_style
            dir_table_data.append([
                Paragraph(metric_name, cell_bold),
                Paragraph(f"{val:,}", table_cell_style)
            ])
            # Alternate row background
            bg = colors.white if idx % 2 == 0 else colors.HexColor("#F8FAFC")
            row_backgrounds.append((idx, bg))
            
        dir_table_style = [
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]
        
        for r_idx, bg_color in row_backgrounds:
            dir_table_style.append(('BACKGROUND', (0, r_idx), (-1, r_idx), bg_color))
            
        dir_table = Table(dir_table_data, colWidths=[300, 200])
        dir_table.setStyle(TableStyle(dir_table_style))
        story.append(dir_table)
    story.append(Spacer(1, 15))


    # =========================================================================
    # SECTION 2: APP USAGE SUMMARY
    # =========================================================================
    story.append(Paragraph("2. App Usage Summary", h1_style))
    story.append(Paragraph("Active Users Usage", h2_style))
    story.append(Paragraph("A breakdown of user activity across major Microsoft 365 services over the last 30, 90, and 180 days, representing actual adoption levels.", body_style))
    story.append(Spacer(1, 8))
    
    o365_usage = data.get("o365_usage", [])
    if not o365_usage:
        story.append(Paragraph("No active user usage report data was available.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        usage_table_data = [[
            Paragraph("Service / License", table_cell_header),
            Paragraph("30 Days Active", table_cell_header),
            Paragraph("90 Days Active", table_cell_header),
            Paragraph("180 Days Active", table_cell_header)
        ]]
        
        for row in o365_usage:
            usage_table_data.append([
                Paragraph(str(row[0]), table_cell_bold),
                Paragraph(f"{row[1]:,}", table_cell_style),
                Paragraph(f"{row[2]:,}", table_cell_style),
                Paragraph(f"{row[3]:,}", table_cell_style)
            ])
            
        usage_table = Table(usage_table_data, colWidths=[200, 100, 100, 100])
        usage_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(usage_table)
    
    # 30-Day Trend Chart - Generated on the fly
    o365_trend = data.get("o365_trend", {})
    if o365_trend and o365_trend.get("dates"):
        try:
            chart_bytes = generate_trend_chart_bytes(o365_trend)
            if chart_bytes:
                story.append(Spacer(1, 15))
                story.append(Paragraph("O365 30-Day Active User Trend", h2_style))
                chart_flow = Image(chart_bytes, width=450, height=210)
                story.append(chart_flow)
        except Exception as chart_ex:
            print(f"Failed to generate active user trend chart for PDF: {chart_ex}")
        
    story.append(PageBreak())

    # M365 Apps Usage
    story.append(Paragraph("Microsoft 365 Client Applications Usage (180 Days)", h2_style))
    story.append(Paragraph("Displays the unique counts of active users on client applications (Outlook, Word, Excel, PowerPoint, OneNote, Teams) segmented by system platforms.", body_style))
    story.append(Spacer(1, 8))
    
    m365_apps = data.get("m365_apps", [])
    if not m365_apps:
         story.append(Paragraph("No client application telemetry data was available.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
         # Format 4-columns layout matching the UI table
         app_table_data = [[
             Paragraph("App / Platform", table_cell_header),
             Paragraph("Active Users", table_cell_header),
             Paragraph("App / Platform", table_cell_header),
             Paragraph("Active Users", table_cell_header)
         ]]
         
         half = (len(m365_apps) + 1) // 2
         left_col = m365_apps[:half]
         right_col = m365_apps[half:]
         
         for r_idx in range(half):
             l_name = left_col[r_idx][0] if r_idx < len(left_col) else ""
             l_val = f"{left_col[r_idx][1]:,}" if r_idx < len(left_col) else ""
             r_name = right_col[r_idx][0] if r_idx < len(right_col) else ""
             r_val = f"{right_col[r_idx][1]:,}" if r_idx < len(right_col) else ""
             
             app_table_data.append([
                 Paragraph(l_name, table_cell_bold if l_name else table_cell_style),
                 Paragraph(l_val, table_cell_style),
                 Paragraph(r_name, table_cell_bold if r_name else table_cell_style),
                 Paragraph(r_val, table_cell_style)
             ])
             
         app_table = Table(app_table_data, colWidths=[150, 100, 150, 100])
         app_table.setStyle(TableStyle([
             ('BACKGROUND', (0, 0), (-1, 0), primary_color),
             ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
             ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
             ('TOPPADDING', (0, 0), (-1, -1), 5),
             ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
             ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
             ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
         ]))
         story.append(app_table)
    
    story.append(Spacer(1, 15))

    # =========================================================================
    # SECTION 3: WORKLOAD STORAGE & METRICS
    # =========================================================================
    story.append(Paragraph("3. Workload Storage & Environmental Telemetry", h1_style))
    story.append(Paragraph("A compiled summary of storage consumption, item counts, and device statistics across Exchange Online, SharePoint, and OneDrive workloads.", body_style))
    story.append(Spacer(1, 8))
    
    # 3.1 Exchange Mailbox & Calendar Telemetry
    story.append(Paragraph("Exchange Online Mailbox & Resource Configurations", h2_style))
    
    mailbox = data.get("mailbox", {})
    calendar = data.get("calendar", {})
    
    # Let's check for warning/errors
    pw_warn = []
    if mailbox.get("powershell_error"):
        pw_warn.append(f"Mailbox: {mailbox['powershell_error']}")
    if calendar.get("powershell_error"):
        pw_warn.append(f"Calendar: {calendar['powershell_error']}")
        
    if pw_warn:
        story.append(Paragraph(f"⚠️ Warning: PowerShell metrics are restricted or incomplete ({'; '.join(pw_warn)})", ParagraphStyle('WarnTxt', parent=body_style, textColor=colors.HexColor("#D97706"), fontName="Helvetica-Bold")))
        story.append(Spacer(1, 4))
        
    workload_table_data = [[
        Paragraph("Metric / Telemetry Property", table_cell_header),
        Paragraph("Exchange Mailbox Value", table_cell_header)
    ]]
    
    # Compile rows from mailbox & calendar
    exchange_rows = [
        ("Total Mailboxes Analyzed", f"{mailbox.get('total_mailboxes', 0):,} Mailboxes"),
        ("Total Size of All Mailboxes", mailbox.get("total_storage_formatted", "0.00 Bytes")),
        ("Average Mailbox Size", mailbox.get("average_mailbox_size_formatted", "0.00 Bytes")),
        ("Total Emails Volume", f"{mailbox.get('total_emails', 0):,} Emails"),
        ("Average Emails per Mailbox", f"{mailbox.get('average_emails', 0.0):,.0f} Emails"),
    ]
    
    s_count = mailbox.get('shared_mailboxes_count')
    s_count_str = f"{s_count:,} Shared Mailboxes" if s_count is not None else "Error/Unavailable"
    s_size_str = mailbox.get("shared_mailboxes_total_formatted", "Error/Unavailable")
    
    pf_count = mailbox.get('public_folders_count')
    pf_count_str = f"{pf_count:,} Public Folders" if pf_count is not None else "Error/Unavailable"
    
    mail_pf_count = mailbox.get('mail_public_folders_count')
    mail_pf_count_str = f"{mail_pf_count:,} Public Folders" if mail_pf_count is not None else "Error/Unavailable"
    
    pf_size_str = mailbox.get("public_folders_total_formatted", "Error/Unavailable")

    exchange_rows += [
        ("Shared Mailboxes Count", s_count_str),
        ("Total Shared Mailbox Size", s_size_str),
        ("Public Folders Count", pf_count_str),
        ("Mail-enabled Public Folders Count", mail_pf_count_str),
        ("Total Public Folder Size", pf_size_str),
    ]
        
    # Add calendar properties
    reserve_val = calendar.get("CanUsersReserveRooms")
    if isinstance(reserve_val, bool): reserve_val = "Yes" if reserve_val else "No"
    
    att_val = calendar.get("CanShareAttachments")
    if isinstance(att_val, bool): att_val = "Yes" if att_val else "No"
    
    exchange_rows += [
        ("Room & Resource Reservation Enabled", str(reserve_val)),
        ("Calendar Resource Pools (Rooms/Devices)", calendar.get("NamingConvention") or "None found"),
        ("Calendar Attachment Link Permissions", str(att_val)),
    ]
    
    for label, val in exchange_rows:
        workload_table_data.append([
            Paragraph(label, table_cell_bold),
            Paragraph(val, table_cell_style)
        ])
        
    ex_table = Table(workload_table_data, colWidths=[260, 240])
    ex_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), primary_color),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
        ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
    ]))
    story.append(ex_table)
    story.append(Spacer(1, 15))

    # 3.1b Integrated Apps
    story.append(Paragraph("Integrated Apps", h2_style))
    story.append(Paragraph("This section lists all organization-wide apps deployed in Exchange Online by administrators and their enabled status.", body_style))
    story.append(Spacer(1, 8))
    
    org_apps = calendar.get("OrganizationApps", [])
    apps_error = calendar.get("AppsError")
    
    if apps_error:
        story.append(Paragraph(f"Error querying organization apps: {apps_error}", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    elif not org_apps:
        story.append(Paragraph("No organization-wide apps found in Exchange Online.", body_style))
    else:
        apps_table_data = [[
            Paragraph("App Display Name", table_cell_header),
            Paragraph("Status", table_cell_header),
            Paragraph("App Display Name", table_cell_header),
            Paragraph("Status", table_cell_header)
        ]]
        half = (len(org_apps) + 1) // 2
        left_col = org_apps[:half]
        right_col = org_apps[half:]
        
        for r_idx in range(half):
            row_items = []
            if r_idx < len(left_col):
                app = left_col[r_idx]
                enabled_str = "Enabled" if app.get("Enabled") else "Disabled"
                row_items.extend([app.get("DisplayName", "-"), enabled_str])
            else:
                row_items.extend(["", ""])
                
            if r_idx < len(right_col):
                app = right_col[r_idx]
                enabled_str = "Enabled" if app.get("Enabled") else "Disabled"
                row_items.extend([app.get("DisplayName", "-"), enabled_str])
            else:
                row_items.extend(["", ""])
                
            apps_table_data.append([
                Paragraph(row_items[0], table_cell_bold if row_items[0] else table_cell_style),
                Paragraph(row_items[1], table_cell_style),
                Paragraph(row_items[2], table_cell_bold if row_items[2] else table_cell_style),
                Paragraph(row_items[3], table_cell_style)
            ])
            
        apps_table = Table(apps_table_data, colWidths=[180, 70, 180, 70])
        apps_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(apps_table)
        
    story.append(Spacer(1, 15))

    # 3.1c Exchange Connectors
    story.append(Paragraph("Exchange Connectors", h2_style))
    story.append(Paragraph("This section displays mail routing connectors configured in Exchange Online.", body_style))
    story.append(Spacer(1, 8))
    
    connectors = data.get("exchange_connectors", [])
    if not connectors:
        story.append(Paragraph("No Exchange connectors configured.", body_style))
    else:
        conn_table_data = [[
            Paragraph("Direction", table_cell_header),
            Paragraph("Connector Name", table_cell_header),
            Paragraph("Status", table_cell_header),
            Paragraph("Domains", table_cell_header),
            Paragraph("Routing Config", table_cell_header)
        ]]
        for conn in connectors:
            routing_txt = conn.get("Routing", "-").replace("\n", "<br/>")
            conn_table_data.append([
                Paragraph(conn.get("Direction", "-"), table_cell_style),
                Paragraph(conn.get("Name", "-"), table_cell_bold),
                Paragraph(conn.get("Status", "-"), table_cell_style),
                Paragraph(conn.get("Domains", "-"), table_cell_style),
                Paragraph(routing_txt, table_cell_style)
            ])
        conn_table = Table(conn_table_data, colWidths=[70, 120, 60, 100, 154])
        conn_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(conn_table)
    story.append(Spacer(1, 15))

    # 3.1d Email Clients & PST Files
    story.append(Paragraph("Email Clients & PST Environment", h2_style))
    story.append(Paragraph("Overview of email client adoption and PST configuration.", body_style))
    story.append(Spacer(1, 8))
    
    email_clients = data.get("email_clients", {})
    if not email_clients:
        story.append(Paragraph("No email client telemetry data available.", body_style))
    else:
        ec_table_data = [[
            Paragraph("Client Type", table_cell_header),
            Paragraph("Active Users", table_cell_header)
        ]]
        rows = [
            ("Outlook on the Web (OWA)", email_clients.get("client_browser", 0)),
            ("Outlook for Windows", email_clients.get("client_win_outlook", 0)),
            ("Outlook for Mac", email_clients.get("client_mac_outlook", 0)),
            ("Apple Mail (macOS)", email_clients.get("client_mac_mail", 0)),
            ("Other Desktop Apps", email_clients.get("client_desktop_other", 0)),
            ("Outlook Mobile (iOS/Android)", email_clients.get("client_mobile_outlook", 0)),
            ("Native / Other Mobile Apps", email_clients.get("client_mobile_other", 0)),
            ("IMAP4 Apps", email_clients.get("client_imap", 0)),
            ("POP3 Apps", email_clients.get("client_pop", 0)),
            ("SMTP Apps", email_clients.get("client_smtp", 0))
        ]
        for label, val in rows:
            ec_table_data.append([
                Paragraph(label, table_cell_bold),
                Paragraph(f"{val:,} Users" if 'SMTP' not in label else f"{val:,} Accounts", table_cell_style)
            ])
        ec_table = Table(ec_table_data, colWidths=[250, 150])
        ec_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(ec_table)
    story.append(Spacer(1, 10))
    
    pst_files = data.get("pst_files", {})
    if pst_files:
        pst_table_data = [[Paragraph("PST Metric", table_cell_header), Paragraph("Value", table_cell_header)]]
        pst_cloud = pst_files.get("pst_cloud_data", {})
        cloud_count = 0
        cloud_bytes = 0
        if pst_cloud and "value" in pst_cloud:
            for item in pst_cloud.get("value", []):
                for hc in item.get("hitsContainers", []):
                    cloud_count += hc.get("total", 0)
                    for hit in hc.get("hits", []):
                        cloud_bytes += int(hit.get("resource", {}).get("size", 0))

        def format_bytes(size):
            for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
                if size < 1024.0: return f"{size:.2f} {unit}"
                size /= 1024.0
            return f"{size:.2f} PB"

        cloud_size_str = f" ({format_bytes(cloud_bytes)})" if cloud_bytes > 0 else ""
        cloud_str = f"{cloud_count:,} Files{cloud_size_str}" if cloud_count > 0 else "None Detected"

        pst_table_data.append([Paragraph("Cloud (SharePoint & OneDrive)", table_cell_bold), Paragraph(cloud_str, table_cell_style)])
        
        pst_table = Table(pst_table_data, colWidths=[250, 150])
        pst_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(pst_table)
    
    story.append(Spacer(1, 15))
    story.append(PageBreak())

    # 3.2 SharePoint & OneDrive Storage
    story.append(Paragraph("SharePoint & OneDrive Environment Telemetry", h2_style))
    story.append(Paragraph("A comparison of file volume, storage consumption, site activity, and active synchronization clients.", body_style))
    story.append(Spacer(1, 8))
    
    sp = data.get("sharepoint", {})
    od = data.get("onedrive", {})
    
    files_table_data = [[
        Paragraph("Metric Property Description", table_cell_header),
        Paragraph("SharePoint Sites (180d)", table_cell_header),
        Paragraph("OneDrive Personal (180d)", table_cell_header)
    ]]
    
    files_rows = [
        ("Total Scope Count (Sites / Accounts)", f"{sp.get('total_sites', 0):,} Sites", f"{od.get('total_accounts', 0):,} Accounts"),
        ("Total Storage Consumed", sp.get("total_storage_formatted", "0.00 Bytes"), od.get("total_storage_formatted", "0.00 Bytes")),
        ("Total Stored File Count", f"{sp.get('total_files', 0):,} Files", f"{od.get('total_files', 0):,} Files"),
        ("Active Files Count (Active %)", f"{sp.get('active_files', 0):,} ({sp.get('active_files_pct', 0.0):.1f}%)", f"{od.get('active_files', 0):,} ({od.get('active_files_pct', 0.0):.1f}%)"),
        ("Users with Sync Client Active", "N/A (SharePoint level)", f"{od.get('sync_users', 0):,} Users ({od.get('sync_users_pct', 0.0):.1f}%)"),
        ("Active OneNote Users", "N/A (SharePoint level)", f"{od.get('onenote_users', 0):,} Users"),
    ]
    
    for label, sp_val, od_val in files_rows:
        files_table_data.append([
            Paragraph(label, table_cell_bold),
            Paragraph(sp_val, table_cell_style),
            Paragraph(od_val, table_cell_style)
        ])
        
    files_table = Table(files_table_data, colWidths=[200, 150, 150])
    files_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), primary_color),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
        ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
    ]))
    story.append(files_table)
    story.append(Spacer(1, 15))

    sp_data_types = data.get("sharepoint_data_types", {})
    if sp_data_types:
        story.append(Paragraph("SharePoint Data Types (Tenant Wide)", h2_style))
        story.append(Paragraph("A global count of major SharePoint components across the tenant.", body_style))
        story.append(Spacer(1, 8))
        
        sp_dt_table_data = [[
            Paragraph("Data Type", table_cell_header),
            Paragraph("Count", table_cell_header)
        ]]
        
        for k, v in [("Document Libraries", sp_data_types.get("Document Libraries", 0)),
                     ("Lists", sp_data_types.get("Lists", 0)),
                     ("Web Pages", sp_data_types.get("Web Pages", 0))]:
            sp_dt_table_data.append([
                Paragraph(k, table_cell_bold),
                Paragraph(f"{v:,}", table_cell_style)
            ])
            
        sp_dt_table = Table(sp_dt_table_data, colWidths=[250, 250])
        sp_dt_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(sp_dt_table)
        story.append(Spacer(1, 15))

    # 3.3 Microsoft Entra Data
    story.append(Paragraph("Microsoft Entra Data", h2_style))
    story.append(Paragraph("This section outlines application sign-in metrics and authentication methods configuration summaries.", body_style))
    story.append(Spacer(1, 8))
    
    entra_data = data.get("devices_apps", {})
    
    # 3.3.2 App Sign Ins
    story.append(Paragraph("<b>App Sign Ins</b>", body_style))
    story.append(Spacer(1, 4))
    
    app_signins = entra_data.get("app_signins", [])
    if not app_signins:
        story.append(Paragraph("No Azure AD application sign-in logs were discovered or permission restricted.", ParagraphStyle('ErrTxtAppSignins', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        app_signins_table_data = [[
            Paragraph("App Name", table_cell_header),
            Paragraph("Successful Sign Ins", table_cell_header)
        ]]
        
        for app, success in app_signins:
            app_signins_table_data.append([
                Paragraph(app, table_cell_bold),
                Paragraph(success, table_cell_style)
            ])
            
        app_signins_table = Table(app_signins_table_data, colWidths=[250, 254])
        app_signins_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(app_signins_table)
        
    story.append(Spacer(1, 15))
    
    # 3.3.3 App Registrations
    story.append(Paragraph("<b>App Registrations</b>", body_style))
    story.append(Spacer(1, 4))
    
    app_registrations = entra_data.get("app_registrations", [])
    if not app_registrations:
        story.append(Paragraph("No Azure AD app registrations were discovered or permission restricted.", ParagraphStyle('ErrTxtAppRegs', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        app_regs_table_data = [[
            Paragraph("App Name", table_cell_header),
            Paragraph("Application ID", table_cell_header),
            Paragraph("Created Date", table_cell_header),
            Paragraph("Sign In Audience", table_cell_header),
            Paragraph("Credentials", table_cell_header)
        ]]
        
        for name, app_id, created, audience, creds in app_registrations:
            formatted_created = created[:10] if created else ""
            app_regs_table_data.append([
                Paragraph(name, table_cell_bold),
                Paragraph(app_id, table_cell_style),
                Paragraph(formatted_created, table_cell_style),
                Paragraph(audience, table_cell_style),
                Paragraph(creds, table_cell_style)
            ])
            
        app_regs_table = Table(app_regs_table_data, colWidths=[120, 110, 70, 110, 94])
        app_regs_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(app_regs_table)
        
    story.append(Spacer(1, 15))
    
    # 3.3.4 User Sign-Ins
    story.append(Paragraph("<b>User Sign-Ins</b>", body_style))
    story.append(Spacer(1, 4))
    
    user_signins = entra_data.get("user_signins", {})
    if not user_signins or (not user_signins.get("apps") and not user_signins.get("os") and not user_signins.get("browsers")):
        story.append(Paragraph("No successful user sign-in logs were discovered or permission restricted.", ParagraphStyle('ErrTxtUserSignins', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        user_signins_table_data = [
            [Paragraph("Sign-in Attribute", table_cell_header), Paragraph("Successful Unique Values", table_cell_header)],
            [Paragraph("App Display Names", table_cell_bold), Paragraph(", ".join(user_signins.get("apps", [])) or "None", table_cell_style)],
            [Paragraph("Operating Systems", table_cell_bold), Paragraph(", ".join(user_signins.get("os", [])) or "None", table_cell_style)],
            [Paragraph("Browsers", table_cell_bold), Paragraph(", ".join(user_signins.get("browsers", [])) or "None", table_cell_style)]
        ]
        user_table = Table(user_signins_table_data, colWidths=[150, 354])
        user_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(user_table)
        story.append(Spacer(1, 4))
        story.append(Paragraph("<font size=8 color='#6B7280'>* Based on sample data collected from signins.</font>", body_style))
        
    story.append(Spacer(1, 15))
    
    # 3.3.5 Authentication Methods
    story.append(Paragraph("<b>Authentication Methods</b>", body_style))
    story.append(Spacer(1, 4))
    
    auth_methods = entra_data.get("auth_methods", [])
    if not auth_methods:
        story.append(Paragraph("No authentication methods logs were discovered or permission restricted.", ParagraphStyle('ErrTxtAuthMethods', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        auth_period = entra_data.get("auth_methods_period", "D7")
        period_str = auth_period
        if period_str.startswith("D"):
            period_str = f"{period_str[1:]} days"
        auth_table_data = [[
            Paragraph("Authentication Method", table_cell_header),
            Paragraph(f"Success Activity Count ({period_str})", table_cell_header)
        ]]
        
        for method, activity in auth_methods:
            auth_table_data.append([
                Paragraph(method, table_cell_bold),
                Paragraph(activity, table_cell_style)
            ])
            
        auth_table = Table(auth_table_data, colWidths=[250, 254])
        auth_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(auth_table)
        
    story.append(Spacer(1, 15))

    # 3.4 Microsoft Intune Data
    story.append(Paragraph("Microsoft Intune Data", h2_style))
    story.append(Paragraph("This section contains mobile applications and device configuration policies managed and distributed via Microsoft Intune.", body_style))
    story.append(Spacer(1, 8))
    
    intune_data = data.get("intune", {})
    mobile_apps = intune_data.get("mobile_apps", [])
    table_rows = intune_data.get("table_rows", [])
    
    # Render Mobile Apps
    story.append(Paragraph("<b>Managed Mobile Apps:</b>", body_style))
    apps_text = ", ".join(mobile_apps) if mobile_apps else "No mobile apps discovered or permission restricted."
    story.append(Paragraph(apps_text, body_style))
    story.append(Spacer(1, 10))
    
    # Render Managed Devices Table (Top 10)
    story.append(Paragraph("<b>Managed Devices (Top 10)</b>", body_style))
    story.append(Spacer(1, 6))
    
    managed_devices = intune_data.get("managed_devices", [])
    if not managed_devices:
        story.append(Paragraph("No managed devices were discovered or permission restricted.", ParagraphStyle('ErrTxtMngDev', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        dev_table_data = [[
            Paragraph("User ID", table_cell_header),
            Paragraph("Device Name", table_cell_header),
            Paragraph("OS", table_cell_header),
            Paragraph("Agent", table_cell_header),
            Paragraph("State", table_cell_header),
            Paragraph("Model", table_cell_header),
            Paragraph("Manufacturer", table_cell_header)
        ]]
        
        for dev in managed_devices[:10]:
            dev_table_data.append([
                Paragraph(dev.get("userId", "N/A"), table_cell_bold),
                Paragraph(dev.get("deviceName", "N/A"), table_cell_style),
                Paragraph(dev.get("operatingSystem", "N/A"), table_cell_style),
                Paragraph(dev.get("managementAgent", "unknown"), table_cell_style),
                Paragraph(dev.get("deviceRegistrationState", "unknown"), table_cell_style),
                Paragraph(dev.get("model", "N/A"), table_cell_style),
                Paragraph(dev.get("manufacturer", "N/A"), table_cell_style)
            ])
            
        dev_table = Table(dev_table_data, colWidths=[90, 80, 60, 70, 70, 64, 70])
        dev_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(dev_table)
        story.append(Spacer(1, 10))

    # Render Device Configurations Table
    story.append(Paragraph("<b>Device Configurations</b>", body_style))
    story.append(Spacer(1, 6))
    
    if not table_rows:
        story.append(Paragraph("No device configuration policies were discovered or permission restricted.", ParagraphStyle('ErrTxtIntune', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        intune_table_data = [[
            Paragraph("Platform", table_cell_header),
            Paragraph("Policy Type", table_cell_header),
            Paragraph("Number of Policies", table_cell_header)
        ]]
        
        for platform, p_type, count in table_rows:
            intune_table_data.append([
                Paragraph(platform, table_cell_bold),
                Paragraph(p_type, table_cell_style),
                Paragraph(count, table_cell_style)
            ])
            
        intune_table = Table(intune_table_data, colWidths=[150, 200, 154])
        intune_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(intune_table)
        story.append(Spacer(1, 4))
        story.append(Paragraph("<font size=8 color='#6B7280'>* Based on sample data collected from Intune.</font>", body_style))
        
    # Render Detected Apps Table (first 10 items)
    story.append(Spacer(1, 10))
    story.append(Paragraph("<b>Detected Apps (Top 10 Discovered)</b>", body_style))
    story.append(Spacer(1, 6))
    
    detected_apps = intune_data.get("detected_apps", [])
    if not detected_apps:
        story.append(Paragraph("No detected apps were discovered or permission restricted.", ParagraphStyle('ErrTxtDetApps', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        det_table_data = [[
            Paragraph("App Name", table_cell_header),
            Paragraph("Version", table_cell_header),
            Paragraph("Publisher", table_cell_header),
            Paragraph("Platform", table_cell_header)
        ]]
        
        for app in detected_apps[:10]:
            det_table_data.append([
                Paragraph(app.get("displayName", "N/A"), table_cell_bold),
                Paragraph(app.get("version", "N/A"), table_cell_style),
                Paragraph(app.get("publisher", "N/A"), table_cell_style),
                Paragraph(app.get("platform", "unknown"), table_cell_style)
            ])
            
        det_table = Table(det_table_data, colWidths=[150, 100, 150, 104])
        det_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(det_table)
        story.append(Spacer(1, 4))
        story.append(Paragraph("<font size=8 color='#6B7280'>* Showing top 10 detected apps. The full inventory list of up to 10,000 apps is available in the exported CSV report.</font>", body_style))
        
    story.append(Spacer(1, 15))

    # =========================================================================
    # SECTION 4: NETWORK SECURITY
    # =========================================================================
    story.append(Paragraph("4. Network Security", h1_style))
    story.append(Paragraph("A summary of Entra Global Secure Access filtering policies, Conditional Access exclusions, and Intune Firewall and Proxy configurations.", body_style))
    story.append(Spacer(1, 10))

    net_sec = data.get("network_security", {})
    filtering_policies = net_sec.get("filtering_policies", [])
    ca_policies = net_sec.get("conditional_access", [])
    fw_policies = net_sec.get("firewall_policies", [])

    # 4.1 Filtering Policies (GSA)
    story.append(Paragraph("Filtering Policies (Global Secure Access)", h2_style))
    if not filtering_policies:
        story.append(Paragraph("No Global Secure Access filtering policies configured or permission restricted.", ParagraphStyle('ErrTxtNetSec', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        table_data = [[
            Paragraph("Policy Name", table_cell_header),
            Paragraph("Description", table_cell_header),
            Paragraph("Version", table_cell_header),
            Paragraph("Action", table_cell_header),
            Paragraph("Rules", table_cell_header)
        ]]
        for item in filtering_policies:
            table_data.append([
                Paragraph(item.get("name", "N/A"), table_cell_bold),
                Paragraph(item.get("description", "N/A"), table_cell_style),
                Paragraph(item.get("version", "N/A"), table_cell_style),
                Paragraph(item.get("action", "N/A"), table_cell_style),
                Paragraph(item.get("rules_count", "0"), table_cell_style)
            ])
        t = Table(table_data, colWidths=[120, 180, 70, 70, 64])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(t)
        
    story.append(Spacer(1, 12))

    # 4.2 Conditional Access policies
    story.append(Paragraph("Conditional Access (Network Exclusions & Scope)", h2_style))
    if not ca_policies:
        story.append(Paragraph("No Conditional Access policies configured or permission restricted.", ParagraphStyle('ErrTxtNetSecCA', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        table_data = [[
            Paragraph("Policy Name", table_cell_header),
            Paragraph("State", table_cell_header),
            Paragraph("Target Users", table_cell_header),
            Paragraph("Target Apps", table_cell_header),
            Paragraph("Grant Controls", table_cell_header)
        ]]
        for item in ca_policies:
            table_data.append([
                Paragraph(item.get("name", "N/A"), table_cell_bold),
                Paragraph(item.get("state", "N/A"), table_cell_style),
                Paragraph(item.get("target_users", "N/A"), table_cell_style),
                Paragraph(item.get("target_apps", "N/A"), table_cell_style),
                Paragraph(item.get("controls", "N/A"), table_cell_style)
            ])
        t = Table(table_data, colWidths=[130, 60, 100, 100, 114])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(t)

    story.append(Spacer(1, 12))

    # 4.3 Firewall/Proxy Policies
    story.append(Paragraph("Firewall and Proxy Configurations", h2_style))
    if not fw_policies:
        story.append(Paragraph("No Firewall or Proxy configurations discovered in Intune policies.", ParagraphStyle('ErrTxtNetSecFW', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        table_data = [[
            Paragraph("Configuration Name", table_cell_header),
            Paragraph("Policy Type", table_cell_header),
            Paragraph("Firewall Status", table_cell_header),
            Paragraph("Proxy Status", table_cell_header)
        ]]
        for item in fw_policies:
            table_data.append([
                Paragraph(item.get("name", "N/A"), table_cell_bold),
                Paragraph(item.get("policy_type", "N/A"), table_cell_style),
                Paragraph(item.get("firewall_status", "N/A"), table_cell_style),
                Paragraph(item.get("proxy_status", "N/A"), table_cell_style)
            ])
        t = Table(table_data, colWidths=[150, 150, 100, 104])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(t)

    story.append(Spacer(1, 15))
    story.append(PageBreak())

    # =========================================================================
    # SECTION 5: DATA SECURITY & GOVERNANCE
    # =========================================================================
    story.append(Paragraph("5. Data Security, Governance & Compliance", h1_style))
    story.append(Paragraph("A summary of classification sensitivity labels and data retention lifecycle policies configured within Microsoft Purview to protect corporate properties.", body_style))
    
    # 4.1 Sensitivity Labels
    story.append(Paragraph("Microsoft Purview Sensitivity Labels", h2_style))
    labels = data.get("security_labels", [])
    if not labels:
        story.append(Paragraph("No Purview Sensitivity Labels configured or permission restricted.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        labels_table_data = [[
            Paragraph("Sensitivity Label", table_cell_header),
            Paragraph("Description", table_cell_header),
            Paragraph("Shield", table_cell_header),
            Paragraph("Mode", table_cell_header),
            Paragraph("Priority", table_cell_header),
            Paragraph("Status", table_cell_header)
        ]]
        
        # Flatten parent labels and sublabels for the PDF table
        flattened_labels = []
        for parent in labels:
            flattened_labels.append({
                "name": parent.get("name", "N/A"),
                "description": parent.get("description", "") or parent.get("toolTip", "") or "N/A",
                "hasProtection": parent.get("hasProtection", False),
                "applicationMode": parent.get("applicationMode", "N/A") or "N/A",
                "priority": parent.get("priority", 0),
                "isEnabled": parent.get("isEnabled", True),
                "is_sub": False
            })
            for sub in parent.get("sublabels", []):
                flattened_labels.append({
                    "name": f"   L_  {sub.get('name', 'N/A')}",
                    "description": sub.get("description", "") or sub.get("toolTip", "") or "N/A",
                    "hasProtection": sub.get("hasProtection", False),
                    "applicationMode": sub.get("applicationMode", "N/A") or "N/A",
                    "priority": sub.get("priority", 0),
                    "isEnabled": sub.get("isEnabled", True),
                    "is_sub": True
                })
                
        for item in flattened_labels:
            bg_bold_s = table_cell_bold if not item["is_sub"] else table_cell_style
            protection_str = "Yes" if item["hasProtection"] else "No"
            status_str = "Enabled" if item["isEnabled"] else "Disabled"
            
            labels_table_data.append([
                Paragraph(item["name"], bg_bold_s),
                Paragraph(item["description"], table_cell_style),
                Paragraph(protection_str, table_cell_style),
                Paragraph(str(item["applicationMode"]).capitalize(), table_cell_style),
                Paragraph(str(item["priority"]), table_cell_style),
                Paragraph(status_str, table_cell_style)
            ])
            
        labels_table = Table(labels_table_data, colWidths=[120, 160, 50, 60, 50, 60])
        labels_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(labels_table)
    
    story.append(PageBreak())

    # 4.2 Retention Policies
    story.append(Paragraph("Microsoft Purview Retention Compliance Policies", h2_style))
    policies = data.get("retention_policies", [])
    if not policies:
        story.append(Paragraph("No Purview Retention compliance policies discovered or permission restricted.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        ret_table_data = [[
            Paragraph("Policy Name", table_cell_header),
            Paragraph("Workloads Involved", table_cell_header),
            Paragraph("Retention Duration Basis", table_cell_header),
            Paragraph("Distribution Status", table_cell_header),
            Paragraph("Status", table_cell_header)
        ]]
        
        policies_list = policies if isinstance(policies, list) else [policies]
        for policy in policies_list:
            duration_val = str(policy.get("Duration", "N/A"))
            duration_str = duration_val
            if duration_val.lower() == "unlimited":
                duration_str = "Keep Forever"
            elif duration_val.isdigit():
                days = int(duration_val)
                if days >= 365:
                    years = days / 365.0
                    duration_str = f"{int(years)} Years ({days} days)" if years.is_integer() else f"{years:.1f} Years ({days} days)"
                else:
                    duration_str = f"{days} days"
            
            trigger_val = policy.get("RetentionTrigger", "N/A")
            if trigger_val and trigger_val != "N/A":
                trigger_map = {"DateCreated": "created date", "DateModified": "last modified date", "DateLabeled": "labeled date"}
                duration_str += f"<br/>(from {trigger_map.get(trigger_val, trigger_val)})"
                
            enabled_val = policy.get("Enabled", True)
            is_enabled = enabled_val.lower() == "true" if isinstance(enabled_val, str) else bool(enabled_val)
            status_str = "Enabled" if is_enabled else "Disabled"
            
            ret_table_data.append([
                Paragraph(policy.get("Name", "N/A"), table_cell_bold),
                Paragraph(policy.get("Workload", "N/A"), table_cell_style),
                Paragraph(duration_str, table_cell_style),
                Paragraph(policy.get("DistributionStatus", "Success"), table_cell_style),
                Paragraph(status_str, table_cell_style)
            ])
            
        ret_table = Table(ret_table_data, colWidths=[130, 110, 110, 90, 60])
        ret_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(ret_table)
    story.append(Spacer(1, 15))

    # 4.3 Data Loss Prevention Policies
    story.append(Paragraph("4.3 Data Loss Prevention Policies", h2_style))
    story.append(Paragraph("This section outlines DLP policies configured in Microsoft Purview to prevent accidental data leaks.", body_style))
    story.append(Spacer(1, 8))
    
    dlp_policies = data.get("dlp_policies", [])
    if not dlp_policies:
        story.append(Paragraph("No Purview Data Loss Prevention policies discovered.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        dlp_table_data = [[
            Paragraph("Policy Name", table_cell_header),
            Paragraph("Mode", table_cell_header),
            Paragraph("Workload", table_cell_header),
            Paragraph("State", table_cell_header),
            Paragraph("Actions", table_cell_header),
            Paragraph("Created By", table_cell_header)
        ]]
        for dlp in dlp_policies:
            en_val = str(dlp.get("Enabled", "")).lower()
            state_str = "Enabled" if en_val in ("true", "1", "yes") else "Disabled"
            
            dlp_table_data.append([
                Paragraph(dlp.get("Name", "-"), table_cell_bold),
                Paragraph(dlp.get("Mode", "-"), table_cell_style),
                Paragraph(dlp.get("Workload", "-"), table_cell_style),
                Paragraph(state_str, table_cell_style),
                Paragraph(dlp.get("Actions", "-"), table_cell_style),
                Paragraph(dlp.get("CreatedBy", "-"), table_cell_style)
            ])
        dlp_table = Table(dlp_table_data, colWidths=[110, 50, 110, 60, 90, 80])
        dlp_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(dlp_table)
    story.append(Spacer(1, 15))

    # 4.4 Sensitive Information Types
    story.append(Paragraph("4.4 Sensitive Information Types", h2_style))
    story.append(Paragraph("This section outlines custom and built-in sensitive information types active in the environment.", body_style))
    story.append(Spacer(1, 8))
    
    sit_types = data.get("sensitive_info_types", [])
    if not sit_types:
        story.append(Paragraph("No Sensitive Information Types discovered.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        sit_table_data = [[
            Paragraph("Name", table_cell_header),
            Paragraph("Type", table_cell_header),
            Paragraph("Confidence", table_cell_header)
        ]]
        for sit in sit_types:
            sit_table_data.append([
                Paragraph(sit.get("Name", "-"), table_cell_bold),
                Paragraph(sit.get("Type", "-"), table_cell_style),
                Paragraph(str(sit.get("RecommendedConfidence", "-")), table_cell_style)
            ])
        sit_table = Table(sit_table_data, colWidths=[280, 100, 120])
        sit_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(sit_table)
    story.append(Spacer(1, 15))

    # =========================================================================

    # 4.5 Mail Security
    story.append(Paragraph("4.5 Mail Security (Exchange)", h2_style))
    story.append(Paragraph("This section displays configured email filtering and threat protection policies.", body_style))
    story.append(Spacer(1, 8))
    
    mail_sec = data.get("mail_security", {})
    if not mail_sec or (not mail_sec.get("defender", {}).get("skus") and not mail_sec.get("eop", {}).get("skus")):
        story.append(Paragraph("No mail security SKUs detected.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        ms_table_data = [[
            Paragraph("Mail Security Configuration", table_cell_header),
            Paragraph("Detected SKUs", table_cell_header),
            Paragraph("Affected Users", table_cell_header)
        ]]
        
        defender_data = mail_sec.get("defender", {})
        eop_data = mail_sec.get("eop", {})
        
        if defender_data.get("skus"):
            ms_table_data.append([
                Paragraph("Microsoft Defender for Office 365", table_cell_bold),
                Paragraph(", ".join(defender_data.get("skus", [])), table_cell_style),
                Paragraph(f"{defender_data.get('users', 0):,} Users", table_cell_style)
            ])
            
        if eop_data.get("skus"):
            ms_table_data.append([
                Paragraph("Exchange Online Protection (Baseline)", table_cell_bold),
                Paragraph(", ".join(eop_data.get("skus", [])), table_cell_style),
                Paragraph(f"{eop_data.get('users', 0):,} Users", table_cell_style)
            ])
            
        ms_table = Table(ms_table_data, colWidths=[200, 200, 100])
        ms_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(ms_table)
    story.append(Spacer(1, 15))

    # 4.6 Exchange Transport Rules
    story.append(Paragraph("4.6 Exchange Transport Rules", h2_style))
    story.append(Paragraph("This section displays mail flow rules configured in Exchange Online.", body_style))
    story.append(Spacer(1, 8))
    
    transport_rules = data.get("transport_rules", [])
    if not transport_rules:
        story.append(Paragraph("No Exchange Transport Rules discovered.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        rules_table_data = [[
            Paragraph("Rule Name", table_cell_header),
            Paragraph("State", table_cell_header),
            Paragraph("Priority", table_cell_header),
            Paragraph("Mode", table_cell_header),
            Paragraph("Rule Logic", table_cell_header)
        ]]
        
        display_rules = transport_rules
        for rule in display_rules:
            desc_text = rule.get("Description") or "N/A"
            rules_table_data.append([
                Paragraph(str(rule.get("Name", "-")), table_cell_bold),
                Paragraph(str(rule.get("State", "-")), table_cell_style),
                Paragraph(str(rule.get("Priority", "-")), table_cell_style),
                Paragraph(str(rule.get("Mode", "-")), table_cell_style),
                Paragraph(str(desc_text), small_table_cell_style)
            ])
            
        rules_table = Table(rules_table_data, colWidths=[120, 50, 40, 60, 234])
        rules_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(rules_table)
    story.append(Spacer(1, 15))

    # 4.7 SSO Service Principals
    story.append(Paragraph("4.7 Enterprise SAML SSO Apps", h2_style))
    story.append(Paragraph("This section displays Enterprise Applications configured for SAML Single Sign-On.", body_style))
    story.append(Spacer(1, 8))
    
    sso_apps = data.get("service_principals_sso", [])
    if not sso_apps:
        story.append(Paragraph("No SAML SSO applications discovered.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        mode_counts = Counter()
        for app in sso_apps:
            mode = app.get("preferredSingleSignOnMode", "").strip() or "None"
            mode_counts[mode] += 1
            
        sso_table_data = [[
            Paragraph("SSO Mode", table_cell_header),
            Paragraph("Number of Applications", table_cell_header)
        ]]
        
        for mode, count in sorted(mode_counts.items(), key=lambda x: x[1], reverse=True):
            sso_table_data.append([
                Paragraph(mode, table_cell_bold),
                Paragraph(f"{count:,} Apps", table_cell_style)
            ])
            
        sso_table = Table(sso_table_data, colWidths=[300, 200])
        sso_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(sso_table)
    story.append(Spacer(1, 15))

    # 4.8 Conditional Access Policies
    story.append(Paragraph("4.8 Conditional Access Policies", h2_style))
    story.append(Paragraph("This section displays Azure AD Auth Policies governing conditional access.", body_style))
    story.append(Spacer(1, 8))
    
    ca_policies = data.get("conditional_access", [])
    if not ca_policies:
        story.append(Paragraph("No conditional access policies discovered.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        ca_table_data = [[
            Paragraph("Policy Name", table_cell_header),
            Paragraph("State", table_cell_header),
            Paragraph("Controls", table_cell_header)
        ]]
        for cap in ca_policies:
            ca_table_data.append([
                Paragraph(cap.get("name", "-"), table_cell_bold),
                Paragraph(cap.get("state", "-"), table_cell_style),
                Paragraph(cap.get("controls", "-"), table_cell_style)
            ])
        ca_table = Table(ca_table_data, colWidths=[250, 100, 150])
        ca_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(ca_table)
    story.append(Spacer(1, 15))
    story.append(PageBreak())

    # 4.9 eDiscovery Cases
    story.append(Paragraph("4.9 Microsoft Purview eDiscovery Cases", h2_style))
    story.append(Paragraph("This section lists the active and closed eDiscovery cases across the tenant, providing visibility into compliance and legal discovery workloads.", body_style))
    story.append(Spacer(1, 8))
    
    ediscovery_cases = data.get("ediscovery_cases", [])
    if not ediscovery_cases:
        story.append(Paragraph("No eDiscovery cases were discovered or Delegated Authentication was not used.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        edisc_table_data = [[
            Paragraph("Display Name", table_cell_header),
            Paragraph("Status", table_cell_header),
            Paragraph("Created Date", table_cell_header),
            Paragraph("Closed By", table_cell_header)
        ]]
        for case in ediscovery_cases[:10]:
            created_date = str(case.get("createdDateTime", "-")).split("T")[0]
            edisc_table_data.append([
                Paragraph(case.get("displayName", "-"), table_cell_bold),
                Paragraph(case.get("status", "-"), table_cell_style),
                Paragraph(created_date, table_cell_style),
                Paragraph(case.get("closedBy", "-"), table_cell_style)
            ])
            
        edisc_table = Table(edisc_table_data, colWidths=[200, 80, 100, 120])
        edisc_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(edisc_table)
        if len(ediscovery_cases) > 10:
             story.append(Paragraph(f"...and {len(ediscovery_cases) - 10} more. See generated CSV reports for full details.", ParagraphStyle('Ital', parent=body_style, fontName='Helvetica-Oblique', textColor=secondary_color)))
    story.append(Spacer(1, 15))
    story.append(PageBreak())

    # SECTION 6: POWER AUTOMATE
    # =========================================================================
    story.append(Paragraph("6. Power Platform & Automate Flows Analytics", h1_style))
    story.append(Paragraph("An analysis of low-code cloud and desktop workflows configured inside the tenant environments, identifying complex workflows and premium connectors.", body_style))
    story.append(Spacer(1, 8))
    
    pa = data.get("power_automate", {})
    if not pa:
        story.append(Paragraph("No Power Platform or Power Automate telemetry scan data was available.", ParagraphStyle('ErrTxt', parent=body_style, textColor=colors.HexColor("#DC2626"))))
    else:
        counts = pa.get("counts", {})
        total_flows = counts.get("Cloud Flows", 0) + counts.get("Desktop Flows", 0)
        premium_conns = pa.get("premium_connectors", [])
        custom_conns = pa.get("custom_connectors", [])
        
        prem_str = ", ".join(premium_conns) if premium_conns else "0"
        cust_str = ", ".join(custom_conns) if custom_conns else "0"
        
        pa_table_data = [[
            Paragraph("Power Platform Telemetry Property", table_cell_header),
            Paragraph("Scanned Value", table_cell_header)
        ]]
        
        pa_rows = [
            ("Total Environments Scanned", str(pa.get("total_environments", 0))),
            ("Total Flows (Active + Inactive)", f"{total_flows:,} Flows"),
            ("Active Cloud Flows Count", f"{pa.get('active_counts', {}).get('Cloud Flows', 0):,} Cloud Flows"),
            ("Active Desktop Flows Count", f"{pa.get('active_counts', {}).get('Desktop Flows', 0):,} Desktop Flows"),
            ("Premium Connectors In Use", prem_str),
            ("Custom Connectors In Use", cust_str),
            ("Complex Business-Logic Flows Identified", f"{len(pa.get('complex_logic_flows', [])):,} Flows"),
        ]
        
        for label, val in pa_rows:
            pa_table_data.append([
                Paragraph(label, table_cell_bold),
                Paragraph(val, table_cell_style)
            ])
            
        pa_table = Table(pa_table_data, colWidths=[220, 280])
        pa_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
            ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
        ]))
        story.append(pa_table)
        
        # Power Automate Breakdown Chart - Generated on the fly
        if counts:
            try:
                pa_chart_bytes = generate_pa_chart_bytes(pa)
                if pa_chart_bytes:
                    story.append(Spacer(1, 15))
                    story.append(Paragraph("Power Automate Flows Breakdown Chart", h2_style))
                    pa_chart = Image(pa_chart_bytes, width=450, height=210)
                    story.append(pa_chart)
            except Exception as chart_ex:
                print(f"Failed to generate Power Automate chart for PDF: {chart_ex}")

    # 4. Build Document
    doc.build(story, canvasmaker=NumberedCanvas)
