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
        "licenses": [],
        "all_sites": []
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
    
    def create_site(site_id, display_name, level, parent_id=None):
        nonlocal site_id_counter, drive_id_counter, list_id_counter
        
        site_item = {
            "id": site_id,
            "displayName": display_name,
            "siteLevel": level,
            "webUrl": f"https://tenant.sharepoint.com/sites/{site_id}",
            "isPersonalSite": False,
            "subsites": [],
            "lists": [],
            "drives": []
        }
        
        delta_site = {
            "id": site_id,
            "displayName": display_name,
            "webUrl": f"https://tenant.sharepoint.com/sites/{site_id}",
            "isPersonalSite": False
        }
        
        if parent_id:
            site_item["parentReference"] = {"siteId": parent_id}
            delta_site["parentReference"] = {"siteId": parent_id}
            
        data["sites"][site_id] = site_item
        data["all_sites"].append(delta_site)
        
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
                "webUrl": f"https://tenant.sharepoint.com/sites/{site_id}/{drive_id}",
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
                "parentReference": {"id": parent_id, "driveId": drive_id, "path": "/root"},
                "fail": random.random() < 0.05
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
            
        min_subsites = 1 if level < 2 else 0
        num_subsites = random.randint(min_subsites, max_subsites_per_site)
        for _ in range(num_subsites):
            site_id = f"site-{site_id_counter}"
            site_id_counter += 1
            create_site(site_id, f"Subsite {site_id_counter}", level + 1, parent_id=parent_id)
            data["sites"][parent_id]["subsites"].append(site_id)
            build_subsites(site_id, level + 1, max_level)

    build_subsites("root", 0, 3)

    # Calculate expected results
    def calculate_expected(ignore_failures=False):
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
    
    def get_subtree_metrics(item_id, current_depth=1, ignore_failures=False):
        item = data["items"][item_id]
        
        if not ignore_failures and item.get("fail", False):
            return {
                "subTreeCount": 0,
                "subTreeSize": 0,
                "maxDepth": 0,
                "fileCount": 0,
                "folderCount": 0,
                "folderCountExceedingDepthLimit": 0,
                "fileCountExceedingDepthLimit": 0,
                "skippedFolderCount": 0,
                "items": []
            }
            
        is_exceeding = current_depth > 3
        
        if "folder" not in item:
            # It's a file
            size = item.get("size", 0)
            return {
                "subTreeCount": 1,
                "subTreeSize": size,
                "maxDepth": 0,
                "fileCount": 1,
                "folderCount": 0,
                "folderCountExceedingDepthLimit": 0,
                "fileCountExceedingDepthLimit": 1 if is_exceeding else 0,
                "skippedFolderCount": 0,
                "items": [item]
            }
            
        # It's a folder
        subtree_count = 1
        subtree_size = 0
        max_depth = 0
        file_count = 0
        folder_count = 1
        folder_exceeding = 1 if is_exceeding else 0
        file_exceeding = 0
        skipped_folders = 1 if "id" not in item.get("parentReference", {}) else 0
        all_items = [item]
        
        for cid in item["children"]:
            c_metrics = get_subtree_metrics(cid, current_depth + 1, ignore_failures)
            subtree_count += c_metrics["subTreeCount"]
            subtree_size += c_metrics["subTreeSize"]
            max_depth = max(max_depth, c_metrics["maxDepth"] + 1)
            file_count += c_metrics["fileCount"]
            folder_count += c_metrics["folderCount"]
            folder_exceeding += c_metrics["folderCountExceedingDepthLimit"]
            file_exceeding += c_metrics["fileCountExceedingDepthLimit"]
            skipped_folders += c_metrics["skippedFolderCount"]
            all_items.extend(c_metrics["items"])
            
        res = {
            "subTreeCount": subtree_count,
            "subTreeSize": subtree_size,
            "maxDepth": max_depth,
            "fileCount": file_count,
            "folderCount": folder_count,
            "folderCountExceedingDepthLimit": folder_exceeding,
            "fileCountExceedingDepthLimit": file_exceeding,
            "skippedFolderCount": skipped_folders,
            "items": all_items
        }
        
        folder_to_metrics[item_id] = res
        print(f"Folder {item_id} subTreeCount: {subtree_count}")
        return res
    # Calculate expected results
    def calculate_expected(ignore_failures=False):
        personal_sites = [s for s in data["sites"].values() if s.get("isPersonalSite", False)]
        team_sites = [s for s in data["sites"].values() if not s.get("isPersonalSite", False)]
        
        expected = {
            "maxEffectiveDepth": 0,
            "maxFolderDepth": 0,
            "maxSubsiteDepth": 0,
            "siteCount": len([s for s in data["sites"].values() if s["siteLevel"] == 0]),
            "subsiteCount": len([s for s in data["sites"].values() if s["siteLevel"] > 0]),
            "personalSiteCount": len(personal_sites),
            "teamSiteCount": len(team_sites),
            "personalSiteDLCount": 0,
            "teamSiteDLCount": 0,
            "shortcutCount": 0,
            "folderCount": 0,
            "fileCount": 0,
            "folderCountExceedingDepthLimit": 0,
            "fileCountExceedingDepthLimit": 0,
            "skippedFolderCount": 0,
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
            "tenantLevelLargeResources": [],
            "tenantLevelLargeResourceCount": 0,
            "siteMetrics": {}
        }
        
        # Initialize expected siteMetrics for root sites (level 0)
        for site_id, site in data["sites"].items():
            if site["siteLevel"] == 0:
                expected["siteMetrics"][site_id] = {
                    "siteLevel": 0,
                    "largeResourceCount": 0,
                    "folderCount": 0,
                    "fileCount": 0,
                    "shortcutCount": 0,
                    "totalSize": 0,
                    "dlCount": 0,
                    "listCount": 0,
                    "subsiteCount": 0,
                    "folderCountExceedingDepthLimit": 0,
                    "fileCountExceedingDepthLimit": 0,
                    "resourceCount": 0
                }
        
        # Initialize buckets for tenant level
        bucket_ranges = [(0, 10240), (10241, 102400), (102401, 1048576), (1048577, float("inf"))]
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
        
        for site in data["sites"].values():
            expected["maxSubsiteDepth"] = max(expected["maxSubsiteDepth"], site["siteLevel"])
            
            for drive_id in site["drives"]:
                drive = data["drives"][drive_id]
                
                drive_metrics = {
                    "maxEffectiveDepth": 0,
                    "folderCount": 0,
                    "fileCount": 0,
                    "shortcutCount": 0,
                    "folderCountExceedingDepthLimit": 0,
                    "fileCountExceedingDepthLimit": 0,
                    "skippedFolderCount": 0,
                    "fileSizeDistribution": {"buckets": []},
                    "largeResources": [],
                    "totalSize": 0
                }
                
                for size_range in bucket_ranges:
                    drive_metrics["fileSizeDistribution"]["buckets"].append({
                        "sizeRange": size_range,
                        "count": 0
                    })
                    
                # Process root items of the drive
                for item_id in drive["items"]:
                    metrics = get_subtree_metrics(item_id, 0, ignore_failures)
                    
                    drive_metrics["folderCount"] += metrics["folderCount"] - 1
                    drive_metrics["fileCount"] += metrics["fileCount"]
                    drive_metrics["shortcutCount"] += len([sub_item for sub_item in metrics["items"] if "remoteItem" in sub_item])
                    drive_metrics["totalSize"] += metrics["subTreeSize"]
                    
                    drive_metrics["maxEffectiveDepth"] = max(drive_metrics["maxEffectiveDepth"], metrics["maxDepth"] - 1)
                    drive_metrics["folderCountExceedingDepthLimit"] += metrics["folderCountExceedingDepthLimit"]
                    drive_metrics["fileCountExceedingDepthLimit"] += metrics["fileCountExceedingDepthLimit"]
                    drive_metrics["skippedFolderCount"] += metrics["skippedFolderCount"]
                    
                    expected["maxEffectiveDepth"] = max(expected["maxEffectiveDepth"], site["siteLevel"] + drive_metrics["maxEffectiveDepth"])
                    expected["maxFolderDepth"] = max(expected["maxFolderDepth"], metrics["maxDepth"] - 1)
                    expected["shortcutCount"] += len([sub_item for sub_item in metrics["items"] if "remoteItem" in sub_item])
                    
                    # File size distribution
                    for sub_item in metrics["items"]:
                        if "file" in sub_item:
                            size_in_kb = sub_item.get("size", 0) / 1024
                            for bucket in drive_metrics["fileSizeDistribution"]["buckets"]:
                                low, high = bucket["sizeRange"]
                                if low <= size_in_kb and size_in_kb <= high:
                                    bucket["count"] += 1
                                    break
                                    
                    # Large resources
                    for sub_item in metrics["items"]:
                        if "folder" in sub_item and sub_item["id"] != item_id: # Skip root
                            f_metrics = folder_to_metrics.get(sub_item["id"])
                            if f_metrics and f_metrics["subTreeCount"] >= 50: # Using limit 50
                                drive_metrics["largeResources"].append({
                                    "type": "FOLDER",
                                    "id": sub_item["id"],
                                    "subTreeCount": f_metrics["subTreeCount"],
                                    "Limit": 50,
                                    "parent": drive_id
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
                expected["folderCountExceedingDepthLimit"] += drive_metrics["folderCountExceedingDepthLimit"]
                expected["fileCountExceedingDepthLimit"] += drive_metrics["fileCountExceedingDepthLimit"]
                expected["skippedFolderCount"] += drive_metrics["skippedFolderCount"]
                expected["driveMetrics"][drive_id] = drive_metrics
        # Aggregate site metrics into root site collections inside expected["siteMetrics"]
        for site_id, site in data["sites"].items():
            curr_site = site
            while "parentReference" in curr_site and "siteId" in curr_site["parentReference"]:
                parent_id = curr_site["parentReference"]["siteId"]
                curr_site = data["sites"][parent_id]
            root_site_id = curr_site["id"]
            
            expected["siteMetrics"][root_site_id]["listCount"] += len(site["lists"])
            
            if site["siteLevel"] > 0:
                expected["siteMetrics"][root_site_id]["subsiteCount"] += 1
                
            for drive_id in site["drives"]:
                if drive_id in expected["driveMetrics"]:
                    drive_metric = expected["driveMetrics"][drive_id]
                    expected["siteMetrics"][root_site_id]["dlCount"] += 1
                    expected["siteMetrics"][root_site_id]["largeResourceCount"] += len(drive_metric["largeResources"])
                    expected["siteMetrics"][root_site_id]["folderCount"] += drive_metric["folderCount"]
                    expected["siteMetrics"][root_site_id]["fileCount"] += drive_metric["fileCount"]
                    expected["siteMetrics"][root_site_id]["shortcutCount"] += drive_metric["shortcutCount"]
                    expected["siteMetrics"][root_site_id]["folderCountExceedingDepthLimit"] += drive_metric["folderCountExceedingDepthLimit"]
                    expected["siteMetrics"][root_site_id]["fileCountExceedingDepthLimit"] += drive_metric["fileCountExceedingDepthLimit"]
                    
                    expected["siteMetrics"][root_site_id]["totalSize"] += drive_metric["totalSize"]

        # Second pass: compute DL, Subsite, and Site Collection large resources
        for site_id, site in data["sites"].items():
            curr_site = site
            while "parentReference" in curr_site and "siteId" in curr_site["parentReference"]:
                parent_id = curr_site["parentReference"]["siteId"]
                curr_site = data["sites"][parent_id]
            root_site_id = curr_site["id"]

            subsite_item_count = 0
            for drive_id in site["drives"]:
                if drive_id in expected["driveMetrics"]:
                    drive_metric = expected["driveMetrics"][drive_id]
                    drive_item_count = drive_metric["folderCount"] + drive_metric["fileCount"]
                    subsite_item_count += drive_item_count
                    
                    # Check if DL is a Large Resource
                    if drive_item_count > 50:
                        expected["siteMetrics"][root_site_id]["largeResourceCount"] += 1
                        expected["tenantLevelLargeResources"].append({
                            "type": "DOCUMENT LIBRARY",
                            "id": drive_id,
                            "subTreeCount": drive_item_count,
                            "parent": site_id,
                            "Limit": 50
                        })

            # Check if Subsite is a Large Resource
            if site["siteLevel"] > 0 and subsite_item_count > 50:
                expected["siteMetrics"][root_site_id]["largeResourceCount"] += 1
                expected["tenantLevelLargeResources"].append({
                    "type": "SUBSITE",
                    "id": site_id,
                    "subTreeCount": subsite_item_count,
                    "parent": root_site_id,
                    "Limit": 50
                })
                    
        for root_site_id, s_metrics in expected["siteMetrics"].items():
            s_metrics["resourceCount"] = s_metrics["folderCount"] + s_metrics["fileCount"] + s_metrics["shortcutCount"]
            
            # Check if Site Collection is a Large Resource
            total_site_count = s_metrics["folderCount"] + s_metrics["fileCount"]
            if total_site_count > 50:
                expected["tenantLevelLargeResources"].append({
                    "type": "SITE COLLECTION",
                    "id": root_site_id,
                    "subTreeCount": total_site_count,
                    "parent": "N/A (Top level site)",
                    "Limit": 50
                })

        for site_id, site in data["sites"].items():
            is_personal = site.get("isPersonalSite", False)
            if is_personal:
                expected["personalSiteDLCount"] += len(site["drives"])
            else:
                expected["teamSiteDLCount"] += len(site["drives"])

        expected["tenantLevelLargeResourceCount"] = len(expected["tenantLevelLargeResources"])

        return expected

    data["expected_result"] = calculate_expected(ignore_failures=True)
    data["expected_result_with_failures"] = calculate_expected(ignore_failures=False)

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
