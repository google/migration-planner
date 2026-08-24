import os

files_to_fix = [
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/data_security_governance.py",
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/active_users_usage.py",
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/devices_apps_telemetry.py",
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/intune_policies.py",
    "/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/exchange_connectors_ui.py"
]

for fp in files_to_fix:
    with open(fp, "r", encoding="utf-8") as f:
        content = f.read()

    # The error is 'pady=0, 10)' which should be 'pady=0'
    content = content.replace("pady=0, 10)", "pady=0")
    content = content.replace("pady=0, 15)", "pady=0")

    with open(fp, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Fixed {fp}")
