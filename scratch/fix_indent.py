import sys

def fix_indent(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    out_lines = []
    in_loop = False

    for i, line in enumerate(lines):
        # We know the loop starts at line 1086 (0-indexed 1085)
        if "for offset, policy in enumerate(page_data, start=1):" in line:
            in_loop = True
            out_lines.append(line)
            continue
            
        if in_loop:
            if line.strip() == "def export_labels_csv(self):":
                in_loop = False
                out_lines.append(line)
                continue
                
            # If the line is empty or just spaces, leave it
            if not line.strip():
                out_lines.append(line)
                continue
                
            # The first two lines of the loop are fine:
            if "r_idx = offset" in line or "bg_style =" in line:
                out_lines.append(line)
                continue

            # Every other line in the loop should be unindented by 4 spaces
            if line.startswith("    "):
                out_lines.append(line[4:])
            else:
                out_lines.append(line)
        else:
            out_lines.append(line)

    with open(file_path, "w", encoding="utf-8") as f:
        f.writelines(out_lines)

if __name__ == "__main__":
    fix_indent("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/data_security_governance.py")
