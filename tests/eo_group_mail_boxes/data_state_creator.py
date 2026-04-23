import json
import random
import os
import argparse

def generate_data(
    num_users=1000,
    output_dir="tests/eo_group_mail_boxes/test_data",
    min_mail_count=500000,
    max_mail_count=1000000,
    seed=None
):
    if seed is not None:
        random.seed(seed)
        
    os.makedirs(output_dir, exist_ok=True)
    
    data = {
        "users": [],
        "userPurpose": {},
        "mailCount": {},
        "expected_result": {}
    }
    
    for i in range(num_users):
        user_id = f"user{i+1}@example.com"
        data["users"].append(user_id)
        data["userPurpose"][user_id] = random.choice(["shared", "user"])
        data["mailCount"][user_id] = random.randint(min_mail_count, max_mail_count)
        data["expected_result"][user_id] = data["mailCount"][user_id] if data["userPurpose"][user_id] == "shared" else 0
        
    filename = f"state_{seed}.json" if seed is not None else "state.json"
    output_path = os.path.join(output_dir, filename)
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Data generated successfully at {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test data for load tests.")
    parser.add_argument("--users", type=int, default=1000, help="Number of users")
    parser.add_argument("--min-mails", type=int, default=500000, help="Min mails per user")
    parser.add_argument("--max-mails", type=int, default=1000000, help="Max mails per user")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for determinism")
    
    args = parser.parse_args()
    
    generate_data(
        num_users=args.users,
        min_mail_count=args.min_mails,
        max_mail_count=args.max_mails,
        seed=args.seed
    )
