import json
import random
import os
import argparse

def generate_data(
    num_sites=20,              # Good number of top-level sites
    max_subsites_per_site=3,   # Creates a tree without exploding
    max_depth=4,               # Deep enough to test folder traversal
    max_drives_per_site=3,     # Multiple drives per site
    max_items_per_folder=20,   # Enough to test pagination (page size is 5)
    output_dir="tests/files/test_data",
    seed=None
):
    if seed is not None:
        random.seed(seed)
        
    os.makedirs(output_dir, exist_ok=True)
    
    data = {
        "root_site": "root",
        "sites": {},
        "lists": {},
        "drives": {},
        "items": {},
        "licenses": []
    }
    
    site_id_counter = 1
    drive_id_counter = 1
    item_id_counter = 1
    list_id_counter = 1
    
    # Generate Licenses
    data["licenses"] = [
        {"appliesTo": "User", "consumedUnits": random.randint(10, 100)},
        {"appliesTo": "Company", "consumedUnits": random.randint(1, 10)}
    ]
    
    def create_site(site_id, display_name, level):
        nonlocal site_id_counter, drive_id_counter, list_id_counter
        
        data["sites"][site_id] = {
            "id": site_id,
            "displayName": display_name,
            "siteLevel": level,
            "subsites": [],
            "lists": [],
            "drives": []
        }
        
        # Create Lists
        num_lists = random.randint(1, 5)
        for _ in range(num_lists):
            list_id = f"list-{list_id_counter}"
            list_id_counter += 1
            data["lists"][list_id] = {"id": list_id, "name": f"List {list_id_counter}"}
            data["sites"][site_id]["lists"].append(list_id)
            
        # Create Drives
        num_drives = random.randint(1, max_drives_per_site)
        for _ in range(num_drives):
            drive_id = f"drive-{drive_id_counter}"
            drive_id_counter += 1
            data["drives"][drive_id] = {
                "id": drive_id,
                "name": f"Drive {drive_id_counter}",
                "driveType": random.choice(["documentLibrary", "personal", "business"]),
                "items": []
            }
            data["sites"][site_id]["drives"].append(drive_id)
            
            # Create a resource for the root folder itself
            root_item_id = f"{drive_id}-root"
            data["items"][root_item_id] = {
                "id": root_item_id,
                "name": "Root",
                "folder": {},
                "children": [],
                "parentReference": {"driveId": drive_id, "path": "/root"}
            }
            
            data["drives"][drive_id]["items"].append(root_item_id)
            
            # Populate Drive with items
            build_folder_tree(drive_id, root_item_id, 0, max_depth)

    def build_folder_tree(drive_id, parent_id, current_depth, max_depth):
        nonlocal item_id_counter
        
        if current_depth >= max_depth:
            return
            
        num_items = random.randint(1, max_items_per_folder)
        for _ in range(num_items):
            item_id = f"item-{item_id_counter}"
            item_id_counter += 1
            
            is_folder = random.random() < 0.3 or current_depth == 0
            
            item = {
                "id": item_id,
                "name": f"Item {item_id_counter}",
                "parentReference": {"id": parent_id, "driveId": drive_id, "path": "/root"}
            }
            
            if is_folder:
                item["folder"] = {}
                item["children"] = []
                item["size"] = 0
                
                if parent_id == "root":
                    data["drives"][drive_id]["items"].append(item_id)
                else:
                    data["items"][parent_id]["children"].append(item_id)
                    
                data["items"][item_id] = item
                build_folder_tree(drive_id, item_id, current_depth + 1, max_depth)
            else:
                item["file"] = {}
                item["size"] = random.randint(100, 10000000) # Size in bytes
                
                if parent_id == "root":
                    data["drives"][drive_id]["items"].append(item_id)
                else:
                    data["items"][parent_id]["children"].append(item_id)
                    
                data["items"][item_id] = item

    # Create Root Site
    create_site("root", "Root Site", 0)
    
    # Create Subsites recursively
    def build_subsites(parent_id, level, max_level):
        nonlocal site_id_counter
        if level >= max_level:
            return
            
        num_subsites = random.randint(0, max_subsites_per_site)
        for _ in range(num_subsites):
            site_id = f"site-{site_id_counter}"
            site_id_counter += 1
            create_site(site_id, f"Subsite {site_id_counter}", level + 1)
            data["sites"][parent_id]["subsites"].append(site_id)
            build_subsites(site_id, level + 1, max_level)

    build_subsites("root", 0, 3)

    # Calculate expected results
    expected = {
        "maxEffectiveDepth": 0,
        "maxFolderDepth": 0,
        "maxSubsiteDepth": 0,
        "subsiteCount": len(data["sites"]),
        "shortcutCount": 0,
        "folderCount": 0,
        "fileCount": 0,
        "listCount": len(data["lists"]),
        "licenseMetrics": {
            "totalLicenseCount": {"User": 0, "Company": 0},
            "consumedUnits": {"User": 0, "Company": 0}
        },
        "driveCounts": {
            "documentLibrary": 0,
            "personal": 0,
            "business": 0,
            "unknown": 0
        },
        "tenantLevelFileSizeDistribution": {"buckets": []},
        "driveMetrics": {},
        "tenantLevelLargeResources": []
    }
    
    # Initialize buckets for tenant level
    bucket_ranges = [(0, 1000), (1001, 10000), (10001, 100000)]
    for size_range in bucket_ranges:
        expected["tenantLevelFileSizeDistribution"]["buckets"].append({
            "sizeRange": size_range,
            "count": 0
        })
        
    # License calculations
    for lic in data["licenses"]:
        applies = lic["appliesTo"]
        expected["licenseMetrics"]["totalLicenseCount"][applies] += 1
        expected["licenseMetrics"]["consumedUnits"][applies] += lic["consumedUnits"]
        
    # Drive counts
    for drive in data["drives"].values():
        dt = drive["driveType"]
        if dt in expected["driveCounts"]:
            expected["driveCounts"][dt] += 1

    folder_to_metrics = {}
    
    def get_subtree_metrics(item_id):
        item = data["items"][item_id]
        
        if "folder" not in item:
            # It's a file
            size = item.get("size", 0)
            return {
                "subTreeCount": 1,
                "subTreeSize": size,
                "maxDepth": 0,
                "fileCount": 1,
                "folderCount": 0,
                "items": [item]
            }
            
        # It's a folder
        subtree_count = 1
        subtree_size = 0
        max_depth = 0
        file_count = 0
        folder_count = 1
        all_items = [item]
        
        for cid in item["children"]:
            c_metrics = get_subtree_metrics(cid)
            subtree_count += c_metrics["subTreeCount"]
            subtree_size += c_metrics["subTreeSize"]
            max_depth = max(max_depth, c_metrics["maxDepth"] + 1)
            file_count += c_metrics["fileCount"]
            folder_count += c_metrics["folderCount"]
            all_items.extend(c_metrics["items"])
            
        res = {
            "subTreeCount": subtree_count,
            "subTreeSize": subtree_size,
            "maxDepth": max_depth,
            "fileCount": file_count,
            "folderCount": folder_count,
            "items": all_items
        }
        
        folder_to_metrics[item_id] = res
        return res

    for site in data["sites"].values():
        expected["maxSubsiteDepth"] = max(expected["maxSubsiteDepth"], site["siteLevel"])
        
        for drive_id in site["drives"]:
            drive = data["drives"][drive_id]
            
            drive_metrics = {
                "maxEffectiveDepth": 0,
                "folderCount": 0,
                "fileCount": 0,
                "shortcutCount": 0,
                "fileSizeDistribution": {"buckets": []},
                "largeResources": []
            }
            
            for size_range in bucket_ranges:
                drive_metrics["fileSizeDistribution"]["buckets"].append({
                    "sizeRange": size_range,
                    "count": 0
                })
                
            # Process root items of the drive
            for item_id in drive["items"]:
                metrics = get_subtree_metrics(item_id)
                
                # Update drive metrics
                drive_max_depth = max(0, metrics["maxDepth"] - 1) # Skip root folder
                drive_metrics["maxEffectiveDepth"] = max(drive_metrics["maxEffectiveDepth"], drive_max_depth)
                drive_metrics["folderCount"] += metrics["folderCount"] - 1 # Skip root
                drive_metrics["fileCount"] += metrics["fileCount"]
                
                expected["maxFolderDepth"] = max(expected["maxFolderDepth"], drive_max_depth)
                expected["maxEffectiveDepth"] = max(expected["maxEffectiveDepth"], site["siteLevel"] + drive_max_depth)
                
                # File size distribution
                for sub_item in metrics["items"]:
                    if "file" in sub_item:
                        size_in_mb = sub_item.get("size", 0) / (1024 * 1024)
                        for bucket in drive_metrics["fileSizeDistribution"]["buckets"]:
                            low, high = bucket["sizeRange"]
                            if low <= size_in_mb and size_in_mb <= high:
                                bucket["count"] += 1
                                break
                                
                # Large resources
                for sub_item in metrics["items"]:
                    if "folder" in sub_item and sub_item["id"] != item_id: # Skip root
                        f_metrics = folder_to_metrics.get(sub_item["id"])
                        if f_metrics and f_metrics["subTreeCount"] >= 2: # Using default limit 2
                            drive_metrics["largeResources"].append({
                                "type": "FOLDER",
                                "id": sub_item["name"],
                                "subTreeCount": f_metrics["subTreeCount"],
                                "Limit": 2,
                                "drive": drive_id
                            })
                            expected["tenantLevelLargeResources"].append(drive_metrics["largeResources"][-1])
                            
            # Aggregate file distribution to tenant level
            for d_bucket in drive_metrics["fileSizeDistribution"]["buckets"]:
                for t_bucket in expected["tenantLevelFileSizeDistribution"]["buckets"]:
                    if d_bucket["sizeRange"] == t_bucket["sizeRange"]:
                        t_bucket["count"] += d_bucket["count"]
                        break
                        
            expected["folderCount"] += drive_metrics["folderCount"]
            expected["fileCount"] += drive_metrics["fileCount"]
            expected["driveMetrics"][drive_id] = drive_metrics

    data["expected_result"] = expected

    filename = f"state_{seed}.json" if seed is not None else "state.json"
    output_path = os.path.join(output_dir, filename)
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Data generated successfully at {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test data for files load tests.")
    parser.add_argument("--sites", type=int, default=10, help="Number of sites")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    
    args = parser.parse_args()
    
    generate_data(num_sites=args.sites, seed=args.seed)
