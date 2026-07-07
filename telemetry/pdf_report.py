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
import html
import logging
from datetime import datetime

logger = logging.getLogger("M365TelemetryAsyncLogger.PdfReport")
from collections import Counter
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.pdfgen import canvas


def escape_text(val) -> str:
    """Safely escapes XML/HTML special characters in dynamic text to prevent ReportLab Paragraph parsing errors."""
    if val is None:
        return "-"
    return html.escape(str(val))



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


def _get_custom_styles(styles, primary_color, secondary_color, text_color):
    # Modify default styles in-place
    styles['Normal'].textColor = text_color
    styles['Normal'].fontSize = 9
    styles['Normal'].leading = 13
    
    custom_styles = {}
    custom_styles['CoverTitle'] = ParagraphStyle(
        'CoverTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=26,
        leading=32,
        textColor=primary_color,
        spaceAfter=10
    )
    
    custom_styles['CoverSubtitle'] = ParagraphStyle(
        'CoverSubtitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=13,
        leading=18,
        textColor=secondary_color,
        spaceAfter=30
    )
    
    custom_styles['SectionH1'] = ParagraphStyle(
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
    
    custom_styles['SectionH2'] = ParagraphStyle(
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
    
    custom_styles['ReportBody'] = ParagraphStyle(
        'ReportBody',
        parent=styles['Normal'],
        fontSize=9,
        leading=13,
        spaceAfter=6
    )
    
    custom_styles['ReportBodyBold'] = ParagraphStyle(
        'ReportBodyBold',
        parent=custom_styles['ReportBody'],
        fontName='Helvetica-Bold'
    )
    
    custom_styles['TableCell'] = ParagraphStyle(
        'TableCell',
        parent=styles['Normal'],
        fontSize=8.5,
        leading=11
    )
    
    custom_styles['TableCellBold'] = ParagraphStyle(
        'TableCellBold',
        parent=custom_styles['TableCell'],
        fontName='Helvetica-Bold',
        textColor=primary_color
    )
    
    custom_styles['TableCellHeader'] = ParagraphStyle(
        'TableCellHeader',
        parent=custom_styles['TableCell'],
        fontName='Helvetica-Bold',
        textColor=colors.white
    )

    custom_styles['SmallTableCell'] = ParagraphStyle(
        'SmallTableCell',
        parent=styles['Normal'],
        fontSize=6.0,
        leading=7.5
    )
    
    custom_styles['SmallTableCellBold'] = ParagraphStyle(
        'SmallTableCellBold',
        parent=custom_styles['SmallTableCell'],
        fontName='Helvetica-Bold',
        textColor=primary_color
    )
    
    custom_styles['SmallTableCellHeader'] = ParagraphStyle(
        'SmallTableCellHeader',
        parent=custom_styles['SmallTableCell'],
        fontName='Helvetica-Bold',
        textColor=colors.white
    )
    
    custom_styles['MetaLabel'] = ParagraphStyle(
        'MetaLabel',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=10,
        textColor=secondary_color
    )
    
    custom_styles['MetaValue'] = ParagraphStyle(
        'MetaValue',
        parent=styles['Normal'],
        fontSize=10,
        textColor=text_color
    )

    custom_styles['SectionErrTxt'] = ParagraphStyle(
        'SectionErrTxt',
        parent=styles['Normal'],
        textColor=colors.HexColor("#DC2626"),
        fontName="Helvetica-Bold",
        fontSize=10,
        spaceBefore=10,
        spaceAfter=10
    )
    
    return custom_styles


def _add_cover_page(story, data, custom_styles, primary_color):
    story.append(Spacer(1, 120))
    story.append(Paragraph("🤝 Deal Assistant", ParagraphStyle('Branding', parent=custom_styles['CoverSubtitle'], fontName='Helvetica-Bold', fontSize=18, textColor=primary_color, spaceAfter=20)))
    story.append(Paragraph("Microsoft 365 Tenant<br/>Audit & Telemetry Report", custom_styles['CoverTitle']))
    story.append(Paragraph("A comprehensive assessment of license allocations, workload adoption patterns, security configurations, and workflow automation.", custom_styles['CoverSubtitle']))
    story.append(Spacer(1, 100))
    
    # Metadata Table
    meta_data = [
        [Paragraph("Tenant Name / ID:", custom_styles['MetaLabel']), Paragraph(escape_text(data.get("tenant_id", "N/A")), custom_styles['MetaValue'])],
        [Paragraph("Report Generated:", custom_styles['MetaLabel']), Paragraph(datetime.now().strftime("%B %d, %Y at %I:%M %p"), custom_styles['MetaValue'])],
        [Paragraph("Assessment Status:", custom_styles['MetaLabel']), Paragraph("🟢 Audit Completed Successfully", ParagraphStyle('StatusStyle', parent=custom_styles['MetaValue'], fontName='Helvetica-Bold', textColor=colors.HexColor("#15803D")))],
        [Paragraph("Report Context:", custom_styles['MetaLabel']), Paragraph("Usage & Adoption Inventory", custom_styles['MetaValue'])]
    ]
    
    meta_table = Table(meta_data, colWidths=[130, 370])
    meta_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('LINEBELOW', (0, 0), (-1, -2), 0.5, colors.HexColor("#F1F5F9")),
    ]))
    story.append(meta_table)


def _add_entra_suite_section(story, data, custom_styles, primary_color, secondary_color, outline_color):
    story.append(Paragraph("1. Microsoft Entra Suite (Identity, Access & Network Security)", custom_styles['SectionH1']))
    story.append(Paragraph("Focus: Governance of the cloud directory footprint, user/application lifecycles, single sign-on parameters, and zero-trust authentication and network perimeters.", custom_styles['ReportBody']))
    story.append(Spacer(1, 10))

    # 1.1 - 1.5: Directory Data
    try:
        dir_data = data.get("directory", {})
        
        # 1.1. Organization Details
        story.append(Paragraph("1.1. Organization Details", custom_styles['SectionH2']))
        story.append(Paragraph("Contains core tenant property variables (tenantType: AAD), sync status attributes, and base provisioned operational plans.", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        org_list = dir_data.get("organization", []) if dir_data else []
        if not org_list:
            story.append(Paragraph("No organization configuration details were available.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            org = org_list[0] if org_list else {}
            plans = org.get("provisionedPlans", [])
            plan_services = sorted(list(set(plan.get("service") for plan in plans if plan.get("service"))))
            plan_services_str = ", ".join(plan_services) if plan_services else "null"
            
            def format_pdf_val(v):
                return "null" if v is None else str(v)
                
            org_table_data = [
                [Paragraph("Property", custom_styles['TableCellHeader']), Paragraph("Value", custom_styles['TableCellHeader'])],
                [Paragraph("displayName", custom_styles['TableCellBold']), Paragraph(escape_text(format_pdf_val(org.get("displayName"))), custom_styles['TableCell'])],
                [Paragraph("isMultipleDataLocationsForServicesEnabled", custom_styles['TableCellBold']), Paragraph(escape_text(format_pdf_val(org.get("isMultipleDataLocationsForServicesEnabled"))), custom_styles['TableCell'])],
                [Paragraph("onPremisesSyncEnabled", custom_styles['TableCellBold']), Paragraph(escape_text(format_pdf_val(org.get("onPremisesSyncEnabled"))), custom_styles['TableCell'])],
                [Paragraph("onPremisesLastSyncDateTime", custom_styles['TableCellBold']), Paragraph(escape_text(format_pdf_val(org.get("onPremisesLastSyncDateTime"))), custom_styles['TableCell'])],
                [Paragraph("partnerTenantType", custom_styles['TableCellBold']), Paragraph(escape_text(format_pdf_val(org.get("partnerTenantType"))), custom_styles['TableCell'])],
                [Paragraph("tenantType", custom_styles['TableCellBold']), Paragraph(escape_text(format_pdf_val(org.get("tenantType"))), custom_styles['TableCell'])],
                [Paragraph("provisionedPlans", custom_styles['TableCellBold']), Paragraph(escape_text(plan_services_str), custom_styles['TableCell'])]
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
            story.append(Paragraph("<font size=8 color='#6B7280'>* If OnPremisesSyncEnabled returns True, on-premises Active Directory is a primary source of truth. If it returns Null or False, the directory is cloud-managed or driven by a 3rd-party application.</font>", custom_styles['ReportBody']))
        story.append(Spacer(1, 15))

        # 1.2. Domains
        story.append(Paragraph("1.2. Domains", custom_styles['SectionH2']))
        story.append(Paragraph("Evaluates internet-facing domain scopes associated with the tenant and maps authentication routing models (Managed vs. Federated).", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        domains = dir_data.get("domains", []) if dir_data else []
        if not domains:
            story.append(Paragraph("No domains discovered in directory scope.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            domains_table_data = [[
                Paragraph("Domain ID", custom_styles['TableCellHeader']),
                Paragraph("Auth Type", custom_styles['TableCellHeader']),
                Paragraph("Admin Managed", custom_styles['TableCellHeader']),
                Paragraph("Default", custom_styles['TableCellHeader']),
                Paragraph("Verified", custom_styles['TableCellHeader']),
                Paragraph("Supported Services", custom_styles['TableCellHeader']),
                Paragraph("Federation Display Name", custom_styles['TableCellHeader']),
                Paragraph("Federation Issuer URI", custom_styles['TableCellHeader'])
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
                    Paragraph(escape_text(item.get("id", "-")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(auth_type), custom_styles['TableCell']),
                    Paragraph(escape_text(admin_managed), custom_styles['TableCell']),
                    Paragraph(escape_text(is_default), custom_styles['TableCell']),
                    Paragraph(escape_text(is_verified), custom_styles['TableCell']),
                    Paragraph(escape_text(services_str), custom_styles['TableCell']),
                    Paragraph(escape_text(fed_idp), custom_styles['TableCell']),
                    Paragraph(escape_text(fed_issuer), custom_styles['TableCell'])
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
            story.append(Paragraph("<font size=8 color='#6B7280'>* AuthenticationType=Managed indicates a cloud managed domain where Microsoft Entra ID performs user authentication. Federated indicates authentication is federated with an identity provider (eg. AD FS, Okta etc.)</font>", custom_styles['ReportBody']))
        story.append(Spacer(1, 15))

        # 1.3. User Creation/Deletion Logs
        story.append(Paragraph("1.3. User Creation/Deletion Logs", custom_styles['SectionH2']))
        story.append(Paragraph("Audits raw activity events tracking user directory provisioning adjustments initiated by management services.", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        user_creation_logs = dir_data.get("user_creation_logs", []) if dir_data else []
        if not user_creation_logs:
            story.append(Paragraph("No user creation or deletion audit logs discovered.", custom_styles['ReportBody']))
        elif user_creation_logs[0].get("activity") == "ERROR":
            err_msg = user_creation_logs[0].get("initiatedBy")
            story.append(Paragraph(f"Error: {escape_text(err_msg)}", ParagraphStyle('ErrTxtUserCreation', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            user_creation_table_data = [[
                Paragraph("Activity", custom_styles['TableCellHeader']),
                Paragraph("Initiated By", custom_styles['TableCellHeader'])
            ]]
            
            for log in user_creation_logs:
                activity = log.get("activity") or "-"
                init_by = log.get("initiatedBy") or "-"
                
                user_creation_table_data.append([
                    Paragraph(escape_text(activity), custom_styles['TableCellBold']),
                    Paragraph(escape_text(init_by), custom_styles['TableCell'])
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
            story.append(Paragraph("<font size=8 color='#6B7280'>* Based on sampled data collected from audit logs.</font>", custom_styles['ReportBody']))
        story.append(Spacer(1, 15))

        # 1.4. Provisioning Logs
        story.append(Paragraph("1.4. Provisioning Logs", custom_styles['SectionH2']))
        story.append(Paragraph("Evaluates directory identity synchronization mechanisms and external source-of-truth status indicators.", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        provisioning_logs = dir_data.get("provisioning_logs", []) if dir_data else []
        if not provisioning_logs:
            story.append(Paragraph("No provisioning audit logs discovered.", custom_styles['ReportBody']))
        elif provisioning_logs[0].get("initiatedBy") == "ERROR":
            err_msg = provisioning_logs[0].get("provisioningAction")
            story.append(Paragraph(f"Error: {escape_text(err_msg)}", ParagraphStyle('ErrTxtProvisioning', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            prov_table_data = [[
                Paragraph("Initiated By", custom_styles['SmallTableCellHeader']),
                Paragraph("Action", custom_styles['SmallTableCellHeader']),
                Paragraph("Steps", custom_styles['SmallTableCellHeader']),
                Paragraph("Service Principal", custom_styles['SmallTableCellHeader']),
                Paragraph("Source System", custom_styles['SmallTableCellHeader']),
                Paragraph("Target System", custom_styles['SmallTableCellHeader']),
                Paragraph("Tenant ID", custom_styles['SmallTableCellHeader']),
                Paragraph("Status Info", custom_styles['SmallTableCellHeader'])
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
                    Paragraph(escape_text(initiatedBy), custom_styles['SmallTableCell']),
                    Paragraph(escape_text(action), custom_styles['SmallTableCellBold']),
                    Paragraph(escape_text(steps), custom_styles['SmallTableCell']),
                    Paragraph(escape_text(sp), custom_styles['SmallTableCell']),
                    Paragraph(escape_text(src), custom_styles['SmallTableCell']),
                    Paragraph(escape_text(tgt), custom_styles['SmallTableCell']),
                    Paragraph(escape_text(tenant), custom_styles['SmallTableCell']),
                    Paragraph(escape_text(statusInfo), custom_styles['SmallTableCell'])
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
            story.append(Paragraph("<font size=8 color='#6B7280'>* Based on sampled data collected from audit logs.</font>", custom_styles['ReportBody']))
        story.append(Spacer(1, 15))

        # 1.5. Groups & Users
        story.append(Paragraph("1.5. Groups & Users", custom_styles['SectionH2']))
        story.append(Paragraph("Tracks total enabled/disabled object accounts, guest profile totals, and directory classification buckets (Unified M365 Groups vs. Static Security Groups).", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        group_counts = dir_data.get("group_counts", {}) if dir_data else {}
        user_counts = dir_data.get("user_counts", {}) if dir_data else {}
        
        dir_table_data = [[
            Paragraph("Category", custom_styles['TableCellHeader']),
            Paragraph("Count", custom_styles['TableCellHeader'])
        ]]
        
        rows_spec = [
            ("Total Users", user_counts.get("total", 0), True),
            ("Enabled Users", user_counts.get("enabled", 0), False),
            ("Disabled Users", user_counts.get("disabled", 0), False),
            ("Member Users", user_counts.get("member", 0), False),
            ("Guest Users", user_counts.get("guest", 0), False),
            ("", "", False),
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
                dir_table_data.append([Paragraph("", custom_styles['TableCell']), Paragraph("", custom_styles['TableCell'])])
                row_backgrounds.append((idx, colors.HexColor("#CBD5E1")))
                continue
                
            cell_bold = custom_styles['TableCellBold'] if is_bold else custom_styles['TableCell']
            dir_table_data.append([
                Paragraph(escape_text(str(metric_name)), cell_bold),
                Paragraph(f"{val:,}", custom_styles['TableCell'])
            ])
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
    except Exception as e:
        logger.exception("Failed to format Directory Summary section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Directory Summary section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 1.6 - 1.9: Microsoft Entra Data
    try:
        entra_data = data.get("devices_apps", {})
        
        # 1.6. Microsoft Entra Data App Sign Ins
        story.append(Paragraph("1.6. Microsoft Entra Data App Sign Ins", custom_styles['SectionH2']))
        story.append(Paragraph("Identifies backend service application access frequencies, logging connection milestones for tools like Azure Portal and Graph Explorer.", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        app_signins = entra_data.get("app_signins", []) if entra_data else []
        if not app_signins:
            story.append(Paragraph("No Azure AD application sign-in logs were discovered or permission restricted.", ParagraphStyle('ErrTxtAppSignins', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            app_signins_table_data = [[
                Paragraph("App Name", custom_styles['TableCellHeader']),
                Paragraph("Successful Sign Ins", custom_styles['TableCellHeader'])
            ]]
            
            for app, success in app_signins:
                app_signins_table_data.append([
                    Paragraph(escape_text(app), custom_styles['TableCellBold']),
                    Paragraph(escape_text(success), custom_styles['TableCell'])
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

        # 1.7. Microsoft Entra Data App Registrations
        story.append(Paragraph("1.7. Microsoft Entra Data App Registrations", custom_styles['SectionH2']))
        story.append(Paragraph("Catalogs tenant-registered application configurations, recording unique application IDs, created dates, and credential properties (secrets/certs).", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        app_registrations = entra_data.get("app_registrations", []) if entra_data else []
        if not app_registrations:
            story.append(Paragraph("No Azure AD app registrations were discovered or permission restricted.", ParagraphStyle('ErrTxtAppRegs', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            app_regs_table_data = [[
                Paragraph("App Name", custom_styles['TableCellHeader']),
                Paragraph("Application ID", custom_styles['TableCellHeader']),
                Paragraph("Created Date", custom_styles['TableCellHeader']),
                Paragraph("Sign In Audience", custom_styles['TableCellHeader']),
                Paragraph("Credentials", custom_styles['TableCellHeader'])
            ]]
            
            for name, app_id, created, audience, creds in app_registrations:
                formatted_created = created[:10] if created else ""
                app_regs_table_data.append([
                    Paragraph(escape_text(name), custom_styles['TableCellBold']),
                    Paragraph(escape_text(app_id), custom_styles['TableCell']),
                    Paragraph(escape_text(formatted_created), custom_styles['TableCell']),
                    Paragraph(escape_text(audience), custom_styles['TableCell']),
                    Paragraph(escape_text(creds), custom_styles['TableCell'])
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

        # 1.8. Microsoft Entra Data User Sign-Ins
        story.append(Paragraph("1.8. Microsoft Entra Data User Sign-Ins", custom_styles['SectionH2']))
        story.append(Paragraph("Mangles raw user access properties, pinpointing target app display names, unique operating systems, and browser version sets.", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        user_signins = entra_data.get("user_signins", {}) if entra_data else {}
        if not user_signins or (not user_signins.get("apps") and not user_signins.get("os") and not user_signins.get("browsers")):
            story.append(Paragraph("No successful user sign-in logs were discovered or permission restricted.", ParagraphStyle('ErrTxtUserSignins', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            user_signins_table_data = [
                [Paragraph("Sign-in Attribute", custom_styles['TableCellHeader']), Paragraph("Successful Unique Values", custom_styles['TableCellHeader'])],
                [Paragraph("App Display Names", custom_styles['TableCellBold']), Paragraph(escape_text(", ".join(user_signins.get("apps", [])) or "None"), custom_styles['TableCell'])],
                [Paragraph("Operating Systems", custom_styles['TableCellBold']), Paragraph(escape_text(", ".join(user_signins.get("os", [])) or "None"), custom_styles['TableCell'])],
                [Paragraph("Browsers", custom_styles['TableCellBold']), Paragraph(escape_text(", ".join(user_signins.get("browsers", [])) or "None"), custom_styles['TableCell'])]
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
            story.append(Paragraph("<font size=8 color='#6B7280'>* Based on sample data collected from signins.</font>", custom_styles['ReportBody']))
        story.append(Spacer(1, 15))

        # 1.9. Microsoft Entra Data Authentication Methods
        story.append(Paragraph("1.9. Microsoft Entra Data Authentication Methods", custom_styles['SectionH2']))
        story.append(Paragraph("Monitors success velocity counts over 7-day intervals across baseline access mechanisms (Password, SMS, OTP).", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        auth_methods = entra_data.get("auth_methods", []) if entra_data else []
        if not auth_methods:
            story.append(Paragraph("No authentication methods logs were discovered or permission restricted.", ParagraphStyle('ErrTxtAuthMethods', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            auth_period = entra_data.get("auth_methods_period", "D7")
            period_str = auth_period
            if period_str.startswith("D"):
                period_str = f"{period_str[1:]} days"
            auth_table_data = [[
                Paragraph("Authentication Method", custom_styles['TableCellHeader']),
                Paragraph(f"Success Activity Count ({period_str})", custom_styles['TableCellHeader'])
            ]]
            
            for method, activity in auth_methods:
                auth_table_data.append([
                    Paragraph(escape_text(method), custom_styles['TableCellBold']),
                    Paragraph(escape_text(activity), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Microsoft Entra Data section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Microsoft Entra Data section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 1.10 - 1.11: Network Security / Global Secure Access
    try:
        net_sec = data.get("network_security", {})
        filtering_policies = net_sec.get("filtering_policies", []) if net_sec else []
        ca_policies_net = net_sec.get("conditional_access", []) if net_sec else []
        
        # 1.10. Filtering Policies (Global Secure Access)
        story.append(Paragraph("1.10. Filtering Policies (Global Secure Access)", custom_styles['SectionH2']))
        story.append(Paragraph("Manages internet edge routing defenses, tracking proxy behavior constraints for web access within Microsoft's SSE architecture.", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        if not filtering_policies:
            story.append(Paragraph("No Global Secure Access filtering policies configured or permission restricted.", ParagraphStyle('ErrTxtNetSec', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            table_data = [[
                Paragraph("Policy Name", custom_styles['TableCellHeader']),
                Paragraph("Description", custom_styles['TableCellHeader']),
                Paragraph("Version", custom_styles['TableCellHeader']),
                Paragraph("Action", custom_styles['TableCellHeader']),
                Paragraph("Rules", custom_styles['TableCellHeader'])
            ]]
            for item in filtering_policies:
                table_data.append([
                    Paragraph(escape_text(item.get("name", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(item.get("description", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(item.get("version", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(item.get("action", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(item.get("rules_count", "0")), custom_styles['TableCell'])
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

        # 1.11. Conditional Access (Network Exclusions & Scope)
        story.append(Paragraph("1.11. Conditional Access (Network Exclusions & Scope)", custom_styles['SectionH2']))
        story.append(Paragraph("Tracks preview rules, target cloud apps, security info enrollment boundaries, and default network exclusions.", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        if not ca_policies_net:
            story.append(Paragraph("No Conditional Access policies configured or permission restricted.", ParagraphStyle('ErrTxtNetSecCA', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            table_data = [[
                Paragraph("Policy Name", custom_styles['TableCellHeader']),
                Paragraph("State", custom_styles['TableCellHeader']),
                Paragraph("Target Users", custom_styles['TableCellHeader']),
                Paragraph("Target Apps", custom_styles['TableCellHeader']),
                Paragraph("Grant Controls", custom_styles['TableCellHeader'])
            ]]
            for item in ca_policies_net:
                table_data.append([
                    Paragraph(escape_text(item.get("name", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(item.get("state", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(item.get("target_users", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(item.get("target_apps", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(item.get("controls", "N/A")), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Network Security GSA/CA section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Network Security GSA/CA section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 1.12 - 1.13: Enterprise SSO & Conditional Access Policies
    try:
        # 1.12. Enterprise SAML SSO Apps
        story.append(Paragraph("1.12. Enterprise SAML SSO Apps", custom_styles['SectionH2']))
        story.append(Paragraph("Evaluates third-party federated applications configured inside the tenant utilizing standard token-based single sign-on properties.", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        sso_apps = data.get("service_principals_sso", [])
        if not sso_apps:
            story.append(Paragraph("No SAML SSO applications discovered.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            mode_counts = Counter()
            for app in sso_apps:
                mode = app.get("preferredSingleSignOnMode", "").strip() or "None"
                mode_counts[mode] += 1
                
            sso_table_data = [[
                Paragraph("SSO Mode", custom_styles['TableCellHeader']),
                Paragraph("Number of Applications", custom_styles['TableCellHeader'])
            ]]
            
            for mode, count in sorted(mode_counts.items(), key=lambda x: x[1], reverse=True):
                sso_table_data.append([
                    Paragraph(escape_text(mode), custom_styles['TableCellBold']),
                    Paragraph(f"{count:,} Apps", custom_styles['TableCell'])
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

        # 1.13. Conditional Access Policies
        story.append(Paragraph("1.13. Conditional Access Policies", custom_styles['SectionH2']))
        story.append(Paragraph("Houses the master policy framework rules requiring multi-factor authentication (MFA) or MDM compliance checks to unlock cloud assets.", custom_styles['ReportBody']))
        story.append(Spacer(1, 8))
        
        ca_policies = data.get("conditional_access", [])
        if not ca_policies:
            story.append(Paragraph("No conditional access policies discovered.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            ca_table_data = [[
                Paragraph("Policy Name", custom_styles['TableCellHeader']),
                Paragraph("State", custom_styles['TableCellHeader']),
                Paragraph("Controls", custom_styles['TableCellHeader'])
            ]]
            for cap in ca_policies:
                ca_table_data.append([
                    Paragraph(escape_text(cap.get("name", "-")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(cap.get("state", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(cap.get("controls", "-")), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format SSO/CA Policies section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting SSO/CA Policies section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))


def _add_m365_core_services_section(story, data, custom_styles, primary_color, secondary_color, outline_color):
    story.append(Paragraph("2. Microsoft 365 Core Services & Adoption (Modern Workplace)", custom_styles['SectionH1']))
    story.append(Paragraph("Focus: Licensing commercial values, operational workspace utilization metrics, storage asset sizes, and productivity application adoption trends across foundational services (Exchange, SharePoint, OneDrive, and Teams).", custom_styles['ReportBody']))
    story.append(Spacer(1, 10))

    # 2.1. Subscribed SKUs
    story.append(Paragraph("2.1. Subscribed SKUs", custom_styles['SectionH2']))
    story.append(Paragraph("Outlines active service license assignments (e.g., SPE_E5, Copilot, Teams Rooms) and compares enabled counts against actual consumed seats.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        sku_list = data.get("skus", [])
        if not sku_list:
            story.append(Paragraph("No subscribed licensing data was discovered or available for this report.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            sku_table_data = [[
                Paragraph("SKU Part Number", custom_styles['TableCellHeader']),
                Paragraph("Allocated Units Status", custom_styles['TableCellHeader']),
                Paragraph("Consumed Units", custom_styles['TableCellHeader'])
            ]]
            
            for item in sku_list:
                sku_table_data.append([
                    Paragraph(escape_text(item.get("skuPartNumber", "UNKNOWN_SKU")), custom_styles['TableCellBold']),
                    Paragraph(format_prepaid_units(item).replace("\n", "<br/>"), custom_styles['TableCell']),
                    Paragraph(f"{item.get('consumedUnits', 0):,}", custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Subscribed SKUs section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Subscribed SKUs section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 2.2. Active Users Usage
    story.append(Paragraph("2.2. Active Users Usage", custom_styles['SectionH2']))
    story.append(Paragraph("Compiles service adoption velocity logs across core application workloads over 30, 90, and 180-day operational windows.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        o365_usage = data.get("o365_usage", [])
        if not o365_usage:
            story.append(Paragraph("No active user usage report data was available.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            usage_table_data = [[
                Paragraph("Service / License", custom_styles['TableCellHeader']),
                Paragraph("30 Days Active", custom_styles['TableCellHeader']),
                Paragraph("90 Days Active", custom_styles['TableCellHeader']),
                Paragraph("180 Days Active", custom_styles['TableCellHeader'])
            ]]
            
            for row in o365_usage:
                usage_table_data.append([
                    Paragraph(escape_text(str(str(row[0]))), custom_styles['TableCellBold']),
                    Paragraph(f"{row[1]:,}", custom_styles['TableCell']),
                    Paragraph(f"{row[2]:,}", custom_styles['TableCell']),
                    Paragraph(f"{row[3]:,}", custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Active Users Usage section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Active Users Usage section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 2.3. O365 30-Day Active User Trend
    story.append(Paragraph("2.3. O365 30-Day Active User Trend", custom_styles['SectionH2']))
    story.append(Paragraph("Maps visual timeline trends displaying consecutive user interaction curves across cloud workspace software platforms.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        o365_trend = data.get("o365_trend", {})
        if not o365_trend or not o365_trend.get("dates"):
            story.append(Paragraph("No active user trend data was available to generate chart.", custom_styles['ReportBody']))
        else:
            chart_bytes = generate_trend_chart_bytes(o365_trend)
            if chart_bytes:
                chart_flow = Image(chart_bytes, width=450, height=210)
                story.append(chart_flow)
    except Exception as e:
        logger.exception("Failed to format O365 30-Day Active User Trend section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting O365 30-Day Active User Trend section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 2.4. Microsoft 365 Client Applications Usage (180 Days)
    story.append(Paragraph("2.4. Microsoft 365 Client Applications Usage (180 Days)", custom_styles['SectionH2']))
    story.append(Paragraph("Breaks down distinct end-user service access endpoints by hardware operating systems (Windows, Mac, Web, Mobile).", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        m365_apps = data.get("m365_apps", [])
        if not m365_apps:
             story.append(Paragraph("No client application telemetry data was available.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
             app_table_data = [[
                 Paragraph("App / Platform", custom_styles['TableCellHeader']),
                 Paragraph("Active Users", custom_styles['TableCellHeader']),
                 Paragraph("App / Platform", custom_styles['TableCellHeader']),
                 Paragraph("Active Users", custom_styles['TableCellHeader'])
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
                     Paragraph(escape_text(str(l_name)), custom_styles['TableCellBold'] if l_name else custom_styles['TableCell']),
                     Paragraph(escape_text(l_val), custom_styles['TableCell']),
                     Paragraph(escape_text(str(r_name)), custom_styles['TableCellBold'] if r_name else custom_styles['TableCell']),
                     Paragraph(escape_text(r_val), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Client Apps Usage section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Client Apps Usage section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 2.5. Exchange Online Mailbox & Resource Configurations
    story.append(Paragraph("2.5. Exchange Online Mailbox & Resource Configurations", custom_styles['SectionH2']))
    story.append(Paragraph("Measures mailbox capacity values, overall sizes, total email counts, shared mailboxes, public folder parameters, and calendar resource pool reservations.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        mailbox = data.get("mailbox", {})
        calendar = data.get("calendar", {})
        
        pw_warn = []
        if mailbox.get("powershell_error"):
            pw_warn.append(f"Mailbox: {mailbox['powershell_error']}")
        if calendar.get("powershell_error"):
            pw_warn.append(f"Calendar: {calendar['powershell_error']}")
            
        if pw_warn:
            story.append(Paragraph(escape_text(f"⚠️ Warning: PowerShell metrics are restricted or incomplete ({'; '.join(pw_warn)})"), ParagraphStyle('WarnTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#D97706"), fontName="Helvetica-Bold")))
            story.append(Spacer(1, 4))
            
        workload_table_data = [[
            Paragraph("Metric / Telemetry Property", custom_styles['TableCellHeader']),
            Paragraph("Exchange Mailbox Value", custom_styles['TableCellHeader'])
        ]]
        
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
                Paragraph(escape_text(label), custom_styles['TableCellBold']),
                Paragraph(escape_text(val), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Exchange Mailbox section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Exchange Mailbox section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 2.6. Integrated Apps
    story.append(Paragraph("2.6. Integrated Apps", custom_styles['SectionH2']))
    story.append(Paragraph("Audits organization-wide mailbox applications and productivity extensions (e.g., Viva Insights, Bing Maps) deployed system-wide by administrators.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        calendar = data.get("calendar", {})
        org_apps = calendar.get("OrganizationApps", []) if calendar else []
        apps_error = calendar.get("AppsError") if calendar else None
        
        if apps_error:
            story.append(Paragraph(f"Error querying organization apps: {escape_text(apps_error)}", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        elif not org_apps:
            story.append(Paragraph("No organization-wide apps found in Exchange Online.", custom_styles['ReportBody']))
        else:
            apps_table_data = [[
                Paragraph("App Display Name", custom_styles['TableCellHeader']),
                Paragraph("Status", custom_styles['TableCellHeader']),
                Paragraph("App Display Name", custom_styles['TableCellHeader']),
                Paragraph("Status", custom_styles['TableCellHeader'])
            ]]
            half = (len(org_apps) + 1) // 2
            left_col = org_apps[:half]
            right_col = org_apps[half:]
            
            for r_idx in range(half):
                row_items = []
                if r_idx < len(left_col):
                    app = left_col[r_idx]
                    enabled_str = "Enabled" if app.get("Enabled") else "Disabled"
                    row_items.extend([escape_text(app.get("DisplayName", "-")), enabled_str])
                else:
                    row_items.extend(["", ""])
                    
                if r_idx < len(right_col):
                    app = right_col[r_idx]
                    enabled_str = "Enabled" if app.get("Enabled") else "Disabled"
                    row_items.extend([escape_text(app.get("DisplayName", "-")), enabled_str])
                else:
                    row_items.extend(["", ""])
                    
                apps_table_data.append([
                    Paragraph(escape_text(str(row_items[0])), custom_styles['TableCellBold'] if row_items[0] else custom_styles['TableCell']),
                    Paragraph(escape_text(str(row_items[1])), custom_styles['TableCell']),
                    Paragraph(escape_text(str(row_items[2])), custom_styles['TableCellBold'] if row_items[2] else custom_styles['TableCell']),
                    Paragraph(escape_text(str(row_items[3])), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Integrated Apps section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Integrated Apps section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 2.7. Exchange Connectors
    story.append(Paragraph("2.7. Exchange Connectors", custom_styles['SectionH2']))
    story.append(Paragraph("Displays mail routing paths, SMTP inbound/outbound connectors, smart host configurations, and partner TLS enforcement rules.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        connectors = data.get("exchange_connectors", [])
        if not connectors:
            story.append(Paragraph("No Exchange connectors configured.", custom_styles['ReportBody']))
        else:
            conn_table_data = [[
                Paragraph("Direction", custom_styles['TableCellHeader']),
                Paragraph("Connector Name", custom_styles['TableCellHeader']),
                Paragraph("Status", custom_styles['TableCellHeader']),
                Paragraph("Domains", custom_styles['TableCellHeader']),
                Paragraph("Routing Config", custom_styles['TableCellHeader'])
            ]]
            for conn in connectors:
                routing_txt = escape_text(conn.get("Routing", "-")).replace("\n", "<br/>")
                conn_table_data.append([
                    Paragraph(escape_text(conn.get("Direction", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(conn.get("Name", "-")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(conn.get("Status", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(conn.get("Domains", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(routing_txt), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Exchange Connectors section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Exchange Connectors section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 2.8. Email Clients & PST Environment
    story.append(Paragraph("2.8. Email Clients & PST Environment", custom_styles['SectionH2']))
    story.append(Paragraph("Pinpoints email client type utilization profiles, tracking unique user footprints across web apps, desktop suites, and legacy protocols.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        email_clients = data.get("email_clients", {})
        if not email_clients:
            story.append(Paragraph("No email client telemetry data available.", custom_styles['ReportBody']))
        else:
            ec_table_data = [[
                Paragraph("Client Type", custom_styles['TableCellHeader']),
                Paragraph("Active Users", custom_styles['TableCellHeader'])
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
                    Paragraph(escape_text(label), custom_styles['TableCellBold']),
                    Paragraph(f"{val:,} Users" if 'SMTP' not in label else f"{val:,} Accounts", custom_styles['TableCell'])
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
            pst_table_data = [[Paragraph("PST Metric", custom_styles['TableCellHeader']), Paragraph("Value", custom_styles['TableCellHeader'])]]
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
    
            pst_table_data.append([Paragraph("Cloud (SharePoint & OneDrive)", custom_styles['TableCellBold']), Paragraph(escape_text(cloud_str), custom_styles['TableCell'])])
            
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
    except Exception as e:
        logger.exception("Failed to format Email Clients & PST section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Email Clients & PST section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 2.9. SharePoint & OneDrive Environment Telemetry
    story.append(Paragraph("2.9. SharePoint & OneDrive Environment Telemetry", custom_styles['SectionH2']))
    story.append(Paragraph("Synthesizes file quantity metrics, total document site spaces, active item splits, and client data sync interactions.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        sp = data.get("sharepoint", {})
        od = data.get("onedrive", {})
        
        files_table_data = [[
            Paragraph("Metric Property Description", custom_styles['TableCellHeader']),
            Paragraph("SharePoint Sites (180d)", custom_styles['TableCellHeader']),
            Paragraph("OneDrive Personal (180d)", custom_styles['TableCellHeader'])
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
                Paragraph(escape_text(label), custom_styles['TableCellBold']),
                Paragraph(escape_text(sp_val), custom_styles['TableCell']),
                Paragraph(escape_text(od_val), custom_styles['TableCell'])
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
            story.append(Paragraph("<b>SharePoint Data Types (Tenant Wide)</b>", custom_styles['ReportBody']))
            story.append(Spacer(1, 4))
            
            sp_dt_table_data = [[
                Paragraph("Data Type", custom_styles['TableCellHeader']),
                Paragraph("Count", custom_styles['TableCellHeader'])
            ]]
            
            for k, v in [("Document Libraries", sp_data_types.get("Document Libraries", 0)),
                         ("Lists", sp_data_types.get("Lists", 0)),
                         ("Web Pages", sp_data_types.get("Web Pages", 0))]:
                sp_dt_table_data.append([
                    Paragraph(escape_text(k), custom_styles['TableCellBold']),
                    Paragraph(f"{v:,}", custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format SharePoint & OneDrive Telemetry section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting SharePoint & OneDrive Telemetry section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 2.10. Microsoft Teams Overview
    story.append(Paragraph("2.10. Microsoft Teams Overview", custom_styles['SectionH2']))
    story.append(Paragraph("Aggregates real-world interaction data from active communication channels, tracking team names, activity timelines, guest levels, and message history.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        msteams_data = data.get("msteams_activity", [])
        if not msteams_data:
            story.append(Paragraph("No Microsoft Teams activity data was available.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            teams_table_data = [[
                Paragraph("Team Name", custom_styles['TableCellHeader']),
                Paragraph("Last Activity", custom_styles['TableCellHeader']),
                Paragraph("Active Users", custom_styles['TableCellHeader']),
                Paragraph("Guests", custom_styles['TableCellHeader']),
                Paragraph("Meetings", custom_styles['TableCellHeader']),
                Paragraph("Messages", custom_styles['TableCellHeader'])
            ]]
            
            for row in msteams_data[:20]:
                teams_table_data.append([
                    Paragraph(escape_text(row.get("Team Name", "-")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(row.get("Last Activity Date", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(row.get("Active Users", "0")), custom_styles['TableCell']),
                    Paragraph(escape_text(row.get("Guests", "0")), custom_styles['TableCell']),
                    Paragraph(escape_text(row.get("Meetings Organized", "0")), custom_styles['TableCell']),
                    Paragraph(escape_text(row.get("Channel Messages", "0")), custom_styles['TableCell'])
                ])
                
            teams_table = Table(teams_table_data, colWidths=[120, 70, 70, 50, 70, 70])
            teams_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(teams_table)
    except Exception as e:
        logger.exception("Failed to format Microsoft Teams Overview section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Microsoft Teams Overview section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))


def _add_intune_section(story, data, custom_styles, primary_color, secondary_color, outline_color):
    story.append(Paragraph("3. Microsoft Intune (Unified Endpoint Management)", custom_styles['SectionH1']))
    story.append(Paragraph("Focus: Managing corporate hardware assets, cross-platform endpoint security baselines, device configuration profiles, and mobile application compliance rules.", custom_styles['ReportBody']))
    story.append(Spacer(1, 10))

    # 3.1. Managed Mobile Apps
    story.append(Paragraph("3.1. Managed Mobile Apps", custom_styles['SectionH2']))
    story.append(Paragraph("Identifies enterprise-monitored mobile applications package distributions (such as App iOS/iPadOS, Flipkart) tracked through application management policies.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        intune_data = data.get("intune", {})
        mobile_apps = intune_data.get("mobile_apps", []) if intune_data else []
        apps_text = ", ".join(mobile_apps) if mobile_apps else "No mobile apps discovered or permission restricted."
        story.append(Paragraph(escape_text(apps_text), custom_styles['ReportBody']))
    except Exception as e:
        logger.exception("Failed to format Managed Mobile Apps section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Managed Mobile Apps section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 3.2. Managed Devices (Top 10)
    story.append(Paragraph("3.2. Managed Devices (Top 10)", custom_styles['SectionH2']))
    story.append(Paragraph("Captures enrollment states for managed endpoints distributed across standard enterprise work groups.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        intune_data = data.get("intune", {})
        managed_devices = intune_data.get("managed_devices", []) if intune_data else []
        if not managed_devices:
            story.append(Paragraph("No managed devices were discovered or permission restricted.", ParagraphStyle('ErrTxtMngDev', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            dev_table_data = [[
                Paragraph("User ID", custom_styles['TableCellHeader']),
                Paragraph("Device Name", custom_styles['TableCellHeader']),
                Paragraph("OS", custom_styles['TableCellHeader']),
                Paragraph("Agent", custom_styles['TableCellHeader']),
                Paragraph("State", custom_styles['TableCellHeader']),
                Paragraph("Model", custom_styles['TableCellHeader']),
                Paragraph("Manufacturer", custom_styles['TableCellHeader'])
            ]]
            
            for dev in managed_devices[:10]:
                dev_table_data.append([
                    Paragraph(escape_text(dev.get("userId", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(dev.get("deviceName", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("operatingSystem", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("managementAgent", "unknown")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("deviceRegistrationState", "unknown")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("model", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("manufacturer", "N/A")), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Managed Devices section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Managed Devices section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 3.3. Video Conferencing (VC) Devices (Top 10)
    story.append(Paragraph("3.3. Video Conferencing (VC) Devices (Top 10)", custom_styles['SectionH2']))
    story.append(Paragraph("Measures corporate meeting room hardware allocations cross-referenced with internal room mailbox accounts.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        intune_data = data.get("intune", {})
        vc_devices = intune_data.get("vc_devices", []) if intune_data else []
        if not vc_devices:
            story.append(Paragraph("No Video Conferencing (VC) devices were discovered or matched against room mailboxes.", ParagraphStyle('ErrTxtVCDev', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            vc_table_data = [[
                Paragraph("User ID", custom_styles['TableCellHeader']),
                Paragraph("Device Name", custom_styles['TableCellHeader']),
                Paragraph("OS", custom_styles['TableCellHeader']),
                Paragraph("Agent", custom_styles['TableCellHeader']),
                Paragraph("State", custom_styles['TableCellHeader']),
                Paragraph("Model", custom_styles['TableCellHeader']),
                Paragraph("Manufacturer", custom_styles['TableCellHeader'])
            ]]
            
            for dev in vc_devices[:10]:
                vc_table_data.append([
                    Paragraph(escape_text(dev.get("userId", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(dev.get("deviceName", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("operatingSystem", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("managementAgent", "unknown")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("deviceRegistrationState", "unknown")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("model", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(dev.get("manufacturer", "N/A")), custom_styles['TableCell'])
                ])
                
            vc_table = Table(vc_table_data, colWidths=[90, 80, 60, 70, 70, 64, 70])
            vc_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(vc_table)
    except Exception as e:
        logger.exception("Failed to format VC Devices section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting VC Devices section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 3.4. Device Configurations
    story.append(Paragraph("3.4. Device Configurations", custom_styles['SectionH2']))
    story.append(Paragraph("Monitors operating system setup profiles, tracking deployment values for platform kiosk, wireless network, and endpoint security configurations.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        intune_data = data.get("intune", {})
        table_rows = intune_data.get("table_rows", []) if intune_data else []
        if not table_rows:
            story.append(Paragraph("No device configuration policies were discovered or permission restricted.", ParagraphStyle('ErrTxtIntune', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            intune_table_data = [[
                Paragraph("Platform", custom_styles['TableCellHeader']),
                Paragraph("Policy Type", custom_styles['TableCellHeader']),
                Paragraph("Number of Policies", custom_styles['TableCellHeader'])
            ]]
            
            for platform, p_type, count in table_rows:
                intune_table_data.append([
                    Paragraph(escape_text(platform), custom_styles['TableCellBold']),
                    Paragraph(escape_text(p_type), custom_styles['TableCell']),
                    Paragraph(escape_text(count), custom_styles['TableCell'])
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
            story.append(Paragraph("<font size=8 color='#6B7280'>* Based on sample data collected from Intune.</font>", custom_styles['ReportBody']))
    except Exception as e:
        logger.exception("Failed to format Device Configurations section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Device Configurations section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 3.5. Mobile Device Compliance Policies
    story.append(Paragraph("3.5. Mobile Device Compliance Policies", custom_styles['SectionH2']))
    story.append(Paragraph("Details compliance baseline rule structures targeting corporate mobile operating systems including Android and iOS.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        intune_data = data.get("intune", {})
        
        # 1. Android Devices Table
        story.append(Paragraph("<i>Android Devices (Top 10)</i>", custom_styles['ReportBody']))
        story.append(Spacer(1, 4))
        android_compliance = intune_data.get("android_compliance", []) if intune_data else []
        if not android_compliance:
            story.append(Paragraph("No Android device compliance policies were discovered or permission restricted.", ParagraphStyle('ErrTxtAndCompliance', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            compliance_table_data = [[
                Paragraph("Display Name", custom_styles['TableCellHeader']),
                Paragraph("Description", custom_styles['TableCellHeader']),
                Paragraph("Created Time", custom_styles['TableCellHeader']),
                Paragraph("Last Modified", custom_styles['TableCellHeader']),
                Paragraph("Version", custom_styles['TableCellHeader'])
            ]]
            for policy in android_compliance[:10]:
                compliance_table_data.append([
                    Paragraph(escape_text(policy.get("displayName", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(policy.get("description", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(policy.get("createdDateTime", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(policy.get("lastModifiedDateTime", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(str(policy.get("version", 0))), custom_styles['TableCell'])
                ])
            compliance_table = Table(compliance_table_data, colWidths=[120, 150, 100, 100, 34])
            compliance_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(compliance_table)
        story.append(Spacer(1, 10))
    
        # 2. iOS Devices Table
        story.append(Paragraph("<i>iOS Devices (Top 10)</i>", custom_styles['ReportBody']))
        story.append(Spacer(1, 4))
        ios_compliance = intune_data.get("ios_compliance", []) if intune_data else []
        if not ios_compliance:
            story.append(Paragraph("No iOS device compliance policies were discovered or permission restricted.", ParagraphStyle('ErrTxtIosCompliance', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            compliance_table_data = [[
                Paragraph("Display Name", custom_styles['TableCellHeader']),
                Paragraph("Description", custom_styles['TableCellHeader']),
                Paragraph("Created Time", custom_styles['TableCellHeader']),
                Paragraph("Last Modified", custom_styles['TableCellHeader']),
                Paragraph("Version", custom_styles['TableCellHeader'])
            ]]
            for policy in ios_compliance[:10]:
                compliance_table_data.append([
                    Paragraph(escape_text(policy.get("displayName", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(policy.get("description", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(policy.get("createdDateTime", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(policy.get("lastModifiedDateTime", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(str(policy.get("version", 0))), custom_styles['TableCell'])
                ])
            compliance_table = Table(compliance_table_data, colWidths=[120, 150, 100, 100, 34])
            compliance_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(compliance_table)
        story.append(Spacer(1, 10))
    
        # 3. Mobile BYOD Configurations
        story.append(Paragraph("<i>Mobile BYOD Configurations (Top 10)</i>", custom_styles['ReportBody']))
        story.append(Spacer(1, 4))
        byod_configs = intune_data.get("byod_configs", []) if intune_data else []
        if not byod_configs:
            story.append(Paragraph("No Mobile BYOD configurations were discovered or permission restricted.", ParagraphStyle('ErrTxtByod', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            byod_table_data = [[
                Paragraph("Display Name", custom_styles['TableCellHeader']),
                Paragraph("Description", custom_styles['TableCellHeader']),
                Paragraph("Priority", custom_styles['TableCellHeader']),
                Paragraph("Last Modified", custom_styles['TableCellHeader']),
                Paragraph("iOS Restrictions", custom_styles['TableCellHeader']),
                Paragraph("Windows Mobile", custom_styles['TableCellHeader']),
                Paragraph("Android Restrictions", custom_styles['TableCellHeader'])
            ]]
            for config in byod_configs[:10]:
                byod_table_data.append([
                    Paragraph(escape_text(config.get("displayName", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(config.get("description", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(str(config.get("priority", 0))), custom_styles['TableCell']),
                    Paragraph(escape_text(config.get("lastModifiedDateTime", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(config.get("iosRestrictions", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(config.get("windowsMobileRestrictions", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(config.get("androidRestrictions", "N/A")), custom_styles['TableCell'])
                ])
            byod_table = Table(byod_table_data, colWidths=[70, 75, 34, 65, 90, 90, 90])
            byod_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(byod_table)
    except Exception as e:
        logger.exception("Failed to format Device Compliance Policies section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Device Compliance Policies section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 3.6. Mobile Device Management Policies (Top 10)
    story.append(Paragraph("3.6. Mobile Device Management Policies (Top 10)", custom_styles['SectionH2']))
    story.append(Paragraph("Documents automated enrollment server URLs, configuration terms-of-use paths, and compliance action redirects.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        intune_data = data.get("intune", {})
        mdm_policies = intune_data.get("mdm_policies", []) if intune_data else []
        if not mdm_policies:
            story.append(Paragraph("No Mobile Device Management (MDM) policies were discovered or permission restricted.", ParagraphStyle('ErrTxtMdmPolicies', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            mdm_table_data = [[
                Paragraph("Display Name", custom_styles['TableCellHeader']),
                Paragraph("Description", custom_styles['TableCellHeader']),
                Paragraph("Applies To", custom_styles['TableCellHeader']),
                Paragraph("Discovery URL", custom_styles['TableCellHeader']),
                Paragraph("Terms of Use", custom_styles['TableCellHeader']),
                Paragraph("Compliance", custom_styles['TableCellHeader'])
            ]]
            for policy in mdm_policies[:10]:
                mdm_table_data.append([
                    Paragraph(escape_text(policy.get("displayName", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(policy.get("description", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(policy.get("appliesTo", "None")), custom_styles['TableCell']),
                    Paragraph(escape_text(policy.get("discoveryUrl", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(policy.get("termsOfUseUrl", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(policy.get("complianceUrl", "N/A")), custom_styles['TableCell'])
                ])
            mdm_table = Table(mdm_table_data, colWidths=[90, 100, 64, 86, 86, 86])
            mdm_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), primary_color),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
                ('GRID', (0, 0), (-1, -1), 0.5, outline_color),
            ]))
            story.append(mdm_table)
    except Exception as e:
        logger.exception("Failed to format MDM Policies section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting MDM Policies section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 3.7. Detected Apps (Top 10 Discovered)
    story.append(Paragraph("3.7. Detected Apps (Top 10 Discovered)", custom_styles['SectionH2']))
    story.append(Paragraph("Logs inventory scans tracking unmanaged or restricted third-party applications operating on managed tenant hardware.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        intune_data = data.get("intune", {})
        detected_apps = intune_data.get("detected_apps", []) if intune_data else []
        if not detected_apps:
            story.append(Paragraph("No detected apps were discovered or permission restricted.", ParagraphStyle('ErrTxtDetApps', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            det_table_data = [[
                Paragraph("App Name", custom_styles['TableCellHeader']),
                Paragraph("Version", custom_styles['TableCellHeader']),
                Paragraph("Publisher", custom_styles['TableCellHeader']),
                Paragraph("Platform", custom_styles['TableCellHeader'])
            ]]
            
            for app in detected_apps[:10]:
                det_table_data.append([
                    Paragraph(escape_text(app.get("displayName", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(app.get("version", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(app.get("publisher", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(app.get("platform", "unknown")), custom_styles['TableCell'])
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
            story.append(Paragraph("<font size=8 color='#6B7280'>* Showing top 10 detected apps. The full inventory list of up to 10,000 apps is available in the exported CSV report.</font>", custom_styles['ReportBody']))
    except Exception as e:
        logger.exception("Failed to format Detected Apps section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Detected Apps section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 3.8. Firewall and Proxy Configurations
    story.append(Paragraph("3.8. Firewall and Proxy Configurations", custom_styles['SectionH2']))
    story.append(Paragraph("Tracks platform configuration profiles governing host hardware firewall states and proxy network interface values.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        net_sec = data.get("network_security", {})
        fw_policies = net_sec.get("firewall_policies", []) if net_sec else []
        if not fw_policies:
            story.append(Paragraph("No Firewall or Proxy configurations discovered in Intune policies.", ParagraphStyle('ErrTxtNetSecFW', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            table_data = [[
                Paragraph("Configuration Name", custom_styles['TableCellHeader']),
                Paragraph("Policy Type", custom_styles['TableCellHeader']),
                Paragraph("Firewall Status", custom_styles['TableCellHeader']),
                Paragraph("Proxy Status", custom_styles['TableCellHeader'])
            ]]
            for item in fw_policies:
                table_data.append([
                    Paragraph(escape_text(item.get("name", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(item.get("policy_type", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(item.get("firewall_status", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(item.get("proxy_status", "N/A")), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Firewall and Proxy section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Firewall and Proxy section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))


def _add_purview_defender_section(story, data, custom_styles, primary_color, secondary_color, outline_color):
    story.append(Paragraph("4. Microsoft Purview & Defender (Compliance, Information Protection & Threat Security)", custom_styles['SectionH1']))
    story.append(Paragraph("Focus: Data life preservation boundaries, content classification frameworks, leak prevention controls (DLP), legal discovery workflows, and email hygiene protections.", custom_styles['ReportBody']))
    story.append(Spacer(1, 10))

    # 4.1. Microsoft Purview Sensitivity Labels
    story.append(Paragraph("4.1. Microsoft Purview Sensitivity Labels", custom_styles['SectionH2']))
    story.append(Paragraph("Coordinates information classification definitions and encryption shields assigned to protect data assets based on priority rules.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        labels = data.get("security_labels", [])
        if not labels:
            story.append(Paragraph("No Purview Sensitivity Labels configured or permission restricted.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            labels_table_data = [[
                Paragraph("Sensitivity Label", custom_styles['TableCellHeader']),
                Paragraph("Description", custom_styles['TableCellHeader']),
                Paragraph("Shield", custom_styles['TableCellHeader']),
                Paragraph("Mode", custom_styles['TableCellHeader']),
                Paragraph("Priority", custom_styles['TableCellHeader']),
                Paragraph("Status", custom_styles['TableCellHeader'])
            ]]
            
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
                bg_bold_s = custom_styles['TableCellBold'] if not item["is_sub"] else custom_styles['TableCell']
                protection_str = "Yes" if item["hasProtection"] else "No"
                status_str = "Enabled" if item["isEnabled"] else "Disabled"
                
                labels_table_data.append([
                    Paragraph(escape_text(item["name"]), bg_bold_s),
                    Paragraph(escape_text(item["description"]), custom_styles['TableCell']),
                    Paragraph(escape_text(str(protection_str)), custom_styles['TableCell']),
                    Paragraph(escape_text(str(item["applicationMode"]).capitalize()), custom_styles['TableCell']),
                    Paragraph(escape_text(str(item["priority"])), custom_styles['TableCell']),
                    Paragraph(escape_text(status_str), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Purview Sensitivity Labels section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Purview Sensitivity Labels section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 4.2. Microsoft Purview Retention Compliance Policies
    story.append(Paragraph("4.2. Microsoft Purview Retention Compliance Policies", custom_styles['SectionH2']))
    story.append(Paragraph("Manages compliance data lifecycles, applying permanent holds or custom retention rules across mail, site, and group workloads.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        policies = data.get("retention_policies", [])
        if not policies:
            story.append(Paragraph("No Purview Retention compliance policies discovered or permission restricted.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            ret_table_data = [[
                Paragraph("Policy Name", custom_styles['TableCellHeader']),
                Paragraph("Workloads Involved", custom_styles['TableCellHeader']),
                Paragraph("Retention Duration Basis", custom_styles['TableCellHeader']),
                Paragraph("Distribution Status", custom_styles['TableCellHeader']),
                Paragraph("Status", custom_styles['TableCellHeader'])
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
                    Paragraph(escape_text(policy.get("Name", "N/A")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(policy.get("Workload", "N/A")), custom_styles['TableCell']),
                    Paragraph(escape_text(str(duration_str)), custom_styles['TableCell']),
                    Paragraph(escape_text(policy.get("DistributionStatus", "Success")), custom_styles['TableCell']),
                    Paragraph(escape_text(status_str), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Purview Retention Compliance section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Purview Retention Compliance section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 4.3. Data Loss Prevention (DLP) Policies
    story.append(Paragraph("4.3. Data Loss Prevention (DLP) Policies", custom_styles['SectionH2']))
    story.append(Paragraph("Outlines active data protection schemas designed to identify, monitor, and automatically block sensitive data transfers across systems.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        dlp_policies = data.get("dlp_policies", [])
        if not dlp_policies:
            story.append(Paragraph("No Purview Data Loss Prevention policies discovered.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            dlp_table_data = [[
                Paragraph("Policy Name", custom_styles['TableCellHeader']),
                Paragraph("Mode", custom_styles['TableCellHeader']),
                Paragraph("Workload", custom_styles['TableCellHeader']),
                Paragraph("State", custom_styles['TableCellHeader']),
                Paragraph("Actions", custom_styles['TableCellHeader']),
                Paragraph("Created By", custom_styles['TableCellHeader'])
            ]]
            for dlp in dlp_policies:
                en_val = str(dlp.get("Enabled", "")).lower()
                state_str = "Enabled" if en_val in ("true", "1", "yes") else "Disabled"
                
                dlp_table_data.append([
                    Paragraph(escape_text(dlp.get("Name", "-")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(dlp.get("Mode", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(dlp.get("Workload", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(str(state_str)), custom_styles['TableCell']),
                    Paragraph(escape_text(dlp.get("Actions", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(dlp.get("CreatedBy", "-")), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format DLP Policies section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting DLP Policies section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 4.4. Sensitive Information Types
    story.append(Paragraph("4.4. Sensitive Information Types", custom_styles['SectionH2']))
    story.append(Paragraph("Catalogs built-in and custom regular expression patterns used to detect confidential strings like banking codes, passport numbers, and API keys.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        sit_types = data.get("sensitive_info_types", [])
        if not sit_types:
            story.append(Paragraph("No Sensitive Information Types discovered.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            sit_table_data = [[
                Paragraph("Name", custom_styles['TableCellHeader']),
                Paragraph("Type", custom_styles['TableCellHeader']),
                Paragraph("Confidence", custom_styles['TableCellHeader'])
            ]]
            for sit in sit_types:
                sit_table_data.append([
                    Paragraph(escape_text(sit.get("Name", "-")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(sit.get("Type", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(str(sit.get("RecommendedConfidence", "-"))), custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Sensitive Info Types section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Sensitive Info Types section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 4.5. Mail Security (Exchange)
    story.append(Paragraph("4.5. Mail Security (Exchange)", custom_styles['SectionH2']))
    story.append(Paragraph("Details active threat filtering protection licensing bundles (such as Microsoft Defender for Office 365 vs. Exchange Online Protection).", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        mail_sec = data.get("mail_security", {})
        if not mail_sec or (not mail_sec.get("defender", {}).get("skus") and not mail_sec.get("eop", {}).get("skus")):
            story.append(Paragraph("No mail security SKUs detected.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            ms_table_data = [[
                Paragraph("Mail Security Configuration", custom_styles['TableCellHeader']),
                Paragraph("Detected SKUs", custom_styles['TableCellHeader']),
                Paragraph("Affected Users", custom_styles['TableCellHeader'])
            ]]
            
            defender_data = mail_sec.get("defender", {})
            eop_data = mail_sec.get("eop", {})
            
            if defender_data.get("skus"):
                ms_table_data.append([
                    Paragraph("Microsoft Defender for Office 365", custom_styles['TableCellBold']),
                    Paragraph(escape_text(", ".join(defender_data.get("skus", []))), custom_styles['TableCell']),
                    Paragraph(f"{defender_data.get('users', 0):,} Users", custom_styles['TableCell'])
                ])
                
            if eop_data.get("skus"):
                ms_table_data.append([
                    Paragraph("Exchange Online Protection (Baseline)", custom_styles['TableCellBold']),
                    Paragraph(escape_text(", ".join(eop_data.get("skus", []))), custom_styles['TableCell']),
                    Paragraph(f"{eop_data.get('users', 0):,} Users", custom_styles['TableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Mail Security section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Mail Security section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 4.6. Exchange Transport Rules
    story.append(Paragraph("4.6. Exchange Transport Rules", custom_styles['SectionH2']))
    story.append(Paragraph("Houses custom mail flow rules designed to govern tenant traffic, manage encryption triggers, enforce domain rejections, and route alerts.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        transport_rules = data.get("transport_rules", [])
        if not transport_rules:
            story.append(Paragraph("No Exchange Transport Rules discovered.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            rules_table_data = [[
                Paragraph("Rule Name", custom_styles['TableCellHeader']),
                Paragraph("State", custom_styles['TableCellHeader']),
                Paragraph("Priority", custom_styles['TableCellHeader']),
                Paragraph("Mode", custom_styles['TableCellHeader']),
                Paragraph("Rule Logic", custom_styles['TableCellHeader'])
            ]]
            
            display_rules = transport_rules
            for rule in display_rules:
                desc_text = rule.get("Description") or "N/A"
                rules_table_data.append([
                    Paragraph(escape_text(rule.get("Name", "-")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(rule.get("State", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(rule.get("Priority", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(rule.get("Mode", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(desc_text), custom_styles['SmallTableCell'])
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
    except Exception as e:
        logger.exception("Failed to format Exchange Transport Rules section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Exchange Transport Rules section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))
    story.append(Spacer(1, 15))

    # 4.7. Microsoft Purview eDiscovery Cases
    story.append(Paragraph("4.7. Microsoft Purview eDiscovery Cases", custom_styles['SectionH2']))
    story.append(Paragraph("Monitors legal lookup spaces, documenting open and closed content searches, hold constraints, and regulatory review timelines.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        ediscovery_cases = data.get("ediscovery_cases", [])
        if not ediscovery_cases:
            story.append(Paragraph("No eDiscovery cases were discovered or Delegated Authentication was not used.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            edisc_table_data = [[
                Paragraph("Display Name", custom_styles['TableCellHeader']),
                Paragraph("Status", custom_styles['TableCellHeader']),
                Paragraph("Created Date", custom_styles['TableCellHeader']),
                Paragraph("Closed By", custom_styles['TableCellHeader'])
            ]]
            for case in ediscovery_cases[:10]:
                created_date = str(case.get("createdDateTime", "-")).split("T")[0]
                edisc_table_data.append([
                    Paragraph(escape_text(case.get("displayName", "-")), custom_styles['TableCellBold']),
                    Paragraph(escape_text(case.get("status", "-")), custom_styles['TableCell']),
                    Paragraph(escape_text(created_date), custom_styles['TableCell']),
                    Paragraph(escape_text(case.get("closedBy", "-")), custom_styles['TableCell'])
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
                 story.append(Paragraph(f"...and {len(ediscovery_cases) - 10} more. See generated CSV reports for full details.", ParagraphStyle('Ital', parent=custom_styles['ReportBody'], fontName='Helvetica-Oblique', textColor=secondary_color)))
    except Exception as e:
        logger.exception("Failed to format eDiscovery Cases section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting eDiscovery Cases section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))


def _add_power_platform_section(story, data, custom_styles, primary_color, secondary_color, outline_color):
    story.append(Paragraph("5. Microsoft Power Platform (Business Applications & Automation)", custom_styles['SectionH1']))
    story.append(Paragraph("Focus: Analytics of citizen development environments, low-code process automated pipelines, environment segmentation structures, and custom connector dependency profiles.", custom_styles['ReportBody']))
    story.append(Spacer(1, 10))

    # 5.1. Power Platform & Automate Flows Analytics
    story.append(Paragraph("5.1. Power Platform & Automate Flows Analytics", custom_styles['SectionH2']))
    story.append(Paragraph("Houses all low-code automation insights, summarizing scanned environment boundaries, active cloud/desktop flow counts, custom/premium integration connectors, and complex logic rule sets.", custom_styles['ReportBody']))
    story.append(Spacer(1, 8))
    
    try:
        pa = data.get("power_automate", {})
        if not pa:
            story.append(Paragraph("No Power Platform or Power Automate telemetry scan data was available.", ParagraphStyle('ErrTxt', parent=custom_styles['ReportBody'], textColor=colors.HexColor("#DC2626"))))
        else:
            counts = pa.get("counts", {})
            total_flows = counts.get("Cloud Flows", 0) + counts.get("Desktop Flows", 0)
            premium_conns = pa.get("premium_connectors", [])
            custom_conns = pa.get("custom_connectors", [])
            
            prem_str = ", ".join(premium_conns) if premium_conns else "0"
            cust_str = ", ".join(custom_conns) if custom_conns else "0"
            
            pa_table_data = [[
                Paragraph("Power Platform Telemetry Property", custom_styles['TableCellHeader']),
                Paragraph("Scanned Value", custom_styles['TableCellHeader'])
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
                    Paragraph(escape_text(label), custom_styles['TableCellBold']),
                    Paragraph(escape_text(str(val)), custom_styles['TableCell'])
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
            
            if counts:
                try:
                    pa_chart_bytes = generate_pa_chart_bytes(pa)
                    if pa_chart_bytes:
                        story.append(Spacer(1, 15))
                        story.append(Paragraph("Power Automate Flows Breakdown Chart", custom_styles['SectionH2']))
                        pa_chart = Image(pa_chart_bytes, width=450, height=210)
                        story.append(pa_chart)
                except Exception as chart_ex:
                    print(f"Failed to generate Power Automate chart for PDF: {chart_ex}")
    except Exception as e:
        logger.exception("Failed to format Power Platform & Automate Flows Analytics section in PDF")
        story.append(Paragraph(f"⚠️ Error formatting Power Platform & Automate Flows Analytics section: {escape_text(str(e))}", custom_styles['SectionErrTxt']))


def generate_pdf_report(data: dict, filepath: str):
    """Generates a beautifully structured PDF document summarizing all tenant telemetry statistics."""
    
    # 1. Document Setup
    doc = SimpleDocTemplate(
        filepath,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=64,
        bottomMargin=64
    )
    
    styles = getSampleStyleSheet()
    
    # Colors
    primary_color = colors.HexColor("#1E3A8A")   # Navy Accent
    secondary_color = colors.HexColor("#475569") # Slate Secondary
    text_color = colors.HexColor("#1E293B")      # Charcoal Body Text
    outline_color = colors.HexColor("#CBD5E1")   # Border light grey
    
    custom_styles = _get_custom_styles(styles, primary_color, secondary_color, text_color)
    
    story = []
    
    # COVER PAGE
    _add_cover_page(story, data, custom_styles, primary_color)
    story.append(PageBreak())
    
    # 1. Microsoft Entra Suite
    _add_entra_suite_section(story, data, custom_styles, primary_color, secondary_color, outline_color)
    story.append(PageBreak())
    
    # 2. Microsoft 365 Core Services & Adoption
    _add_m365_core_services_section(story, data, custom_styles, primary_color, secondary_color, outline_color)
    story.append(PageBreak())
    
    # 3. Microsoft Intune
    _add_intune_section(story, data, custom_styles, primary_color, secondary_color, outline_color)
    story.append(PageBreak())
    
    # 4. Microsoft Purview & Defender
    _add_purview_defender_section(story, data, custom_styles, primary_color, secondary_color, outline_color)
    story.append(PageBreak())
    
    # 5. Microsoft Power Platform
    _add_power_platform_section(story, data, custom_styles, primary_color, secondary_color, outline_color)
    
    # Build Document
    doc.build(story, canvasmaker=NumberedCanvas)
