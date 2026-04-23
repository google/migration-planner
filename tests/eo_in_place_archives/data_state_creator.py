import json
import random
import os
import argparse

def generate_data(
    num_users=1000,
    output_dir="tests/eo_in_place_archives/test_data",
    min_top_folders=10,
    max_top_folders=20,
    min_child_folders=5,
    max_child_folders=30,
    min_depth=6,
    max_depth=15,
    min_mails=500000,
    max_mails=1000000,
    seed=None
):
    if seed is not None:
        random.seed(seed)
        
    os.makedirs(output_dir, exist_ok=True)
    
    data = {
        "users": {},
        "mailboxes": {},
        "folders": {}
    }
    folder_id_counter = 1
    
    for i in range(num_users):
        user_id = f"user{i+1}@example.com"
        mailbox_id = f"archive-{i+1}"
        data["users"][user_id] = mailbox_id
        data["mailboxes"][mailbox_id] = []
        
        target_mails = random.randint(min_mails, max_mails)
        remaining_mails = target_mails
        
        num_top_folders = random.randint(min_top_folders, max_top_folders)
        top_folders = []
        
        for j in range(num_top_folders):
            folder_id = f"folder-{folder_id_counter}"
            folder_id_counter += 1
            data["mailboxes"][mailbox_id].append(folder_id)
            top_folders.append(folder_id)
            
            data["folders"][folder_id] = {
                "id": folder_id,
                "totalItemCount": 0,
                "childFolderCount": 0,
                "childFolders": [],
                "fail": random.random() < 0.05
            }
            
        def build_tree(folder_id, depth, current_max_depth):
            nonlocal remaining_mails, folder_id_counter
            
            if depth >= current_max_depth or remaining_mails <= 0:
                return
                
            num_children = random.randint(min_child_folders, max_child_folders) if depth < current_max_depth - 1 else 0
            
            for _ in range(num_children):
                child_id = f"folder-{folder_id_counter}"
                folder_id_counter += 1
                
                data["folders"][folder_id]["childFolders"].append(child_id)
                data["folders"][folder_id]["childFolderCount"] += 1
                
                mails = random.randint(0, min(100000, remaining_mails)) if remaining_mails > 0 else 0
                remaining_mails -= mails
                
                data["folders"][child_id] = {
                    "id": child_id,
                    "totalItemCount": mails,
                    "childFolderCount": 0,
                    "childFolders": [],
                    "fail": random.random() < 0.05
                }
                
                build_tree(child_id, depth + 1, current_max_depth)
                
        for tf in top_folders:
            mails = random.randint(0, min(50000, remaining_mails)) if remaining_mails > 0 else 0
            remaining_mails -= mails
            data["folders"][tf]["totalItemCount"] = mails
            
            current_max_depth = random.randint(min_depth, max_depth)
            build_tree(tf, 1, current_max_depth)
            
        if remaining_mails > 0 and top_folders:
            data["folders"][top_folders[-1]]["totalItemCount"] += remaining_mails
            remaining_mails = 0

    # Calculate expected results
    data["expected_result"] = {}
    data["expected_result_with_failures"] = {}
    
    def calculate_counts(folder_id, fail_simulation):
        folder = data["folders"][folder_id]
        count = folder["totalItemCount"]
        
        if fail_simulation and folder.get("fail", False):
            return count
            
        for child_id in folder["childFolders"]:
            count += calculate_counts(child_id, fail_simulation)
        return count

    for user_id, mailbox_id in data["users"].items():
        ideal_count = 0
        failure_count = 0
        for folder_id in data["mailboxes"][mailbox_id]:
            ideal_count += calculate_counts(folder_id, False)
            failure_count += calculate_counts(folder_id, True)
        data["expected_result"][user_id] = ideal_count
        data["expected_result_with_failures"][user_id] = failure_count

    filename = f"state_{seed}.json" if seed is not None else "state.json"
    output_path = os.path.join(output_dir, filename)
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Data generated successfully at {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test data for load tests.")
    parser.add_argument("--users", type=int, default=1000, help="Number of users")
    parser.add_argument("--min-top", type=int, default=10, help="Min top level folders per user")
    parser.add_argument("--max-top", type=int, default=20, help="Max top level folders per user")
    parser.add_argument("--min-child", type=int, default=5, help="Min child folders per folder")
    parser.add_argument("--max-child", type=int, default=30, help="Max child folders per folder")
    parser.add_argument("--min-depth", type=int, default=6, help="Min tree depth")
    parser.add_argument("--max-depth", type=int, default=15, help="Max tree depth")
    parser.add_argument("--min-mails", type=int, default=500000, help="Min mails per user")
    parser.add_argument("--max-mails", type=int, default=1000000, help="Max mails per user")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for determinism")
    
    args = parser.parse_args()
    
    generate_data(
        num_users=args.users,
        min_top_folders=args.min_top,
        max_top_folders=args.max_top,
        min_child_folders=args.min_child,
        max_child_folders=args.max_child,
        min_depth=args.min_depth,
        max_depth=args.max_depth,
        min_mails=args.min_mails,
        max_mails=args.max_mails,
        seed=args.seed
    )
