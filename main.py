import json
import os
import sys
import time

import requests

sys.path.append(os.getcwd())
from utils import ( # noqa
    check_date,
    convert_to_mb,
    create_database_session,
    delete_files_in_directory,
    extract_from_json_and_add_to_db,
    find_file,
    generate_md5_hash,
    get_env_variables,
    get_iconverted_value,
    insert_to_spiderrecord_database,
    parse_date,
)

start_time = time.time()
script_path = os.path.abspath(__file__)
script_directory = os.path.dirname(script_path)
env_path = os.path.join(script_directory, ".env")
[
    ecgains,
    module_name,
    base_url,
    executable_path,
    download_path,
    server_path,
    json_path,
    browser_type,
    smi_data_url,
    smi_record_url,
    region_name,
    endpoint_url,
    aws_access_key_id,
    aws_secret_access_key
] = get_env_variables(env_path=env_path)

bid_details = {
    "ecgains": ecgains,
    "module_name": module_name,
    "base_url": base_url,
    "download_path": download_path,
    "server_path": server_path
}

response = requests.get(url="https://opportunity-to-bid.kctcsweb.com/api/public/bids")

parsed_json_data =json.loads(response.text)

def get_bid_data(bid_item: dict, bid_index: int) -> tuple:
    bid_list: list = bid_item["bids"]

    if len(bid_list) == 0:
        return None, bid_index
    
    bid_info = {}
    for bid in bid_list:
        # BID DUE DATE
        bid_due_date: str = bid["end_date"]
        parsed_due_date = parse_date(date=bid_due_date)
        if check_date(date=parsed_due_date):
            continue

        # BID NO
        bid_id: str = bid["bid_number"]

        # BID TITLE
        bid_title: str = bid["title"]

        bid_files: list = bid["media"]

        files_info = download_files(bid_files=bid_files, bid_id=bid_id)
        
        bid_info[bid_index] = {
            "bid_no": bid_id,
            "bid_title": bid_title,
            "bid_due_date": parsed_due_date,
            "files_info": files_info
        }

        bid_index += 1

    return bid_info, bid_index


def download_files(bid_files: list, bid_id: str) -> dict:
    files_info = {}

    for idx, bid_file in enumerate(bid_files, start=1):
        file_id: int = bid_file["id"]
        file_name: str = bid_file["file_name"]
        file_size_in_mb: int = convert_to_mb(size=bid_file["size"], original_unit="B")

        download_url = f"https://opportunity-to-bid.kctcsweb.com/storage/{file_id}/{file_name}"
        
        response = requests.get(url=download_url)

        with open(os.path.join(download_path, file_name), "wb") as file:
            file.write(response.content)

        _, old_file_name, new_file_name = find_file(file_directory=download_path, file_name=file_name)
        hash = generate_md5_hash(ecgain=ecgains, bidno=bid_id, filename=new_file_name)
        iconverted = get_iconverted_value(filename=file_name)

        files_info[idx] = {
            "file_name": old_file_name,
            "new_file_name": new_file_name,
            "file_size_in_mb": str(file_size_in_mb) + " MB",
            "file_url": download_url,
            "hash": hash,
            "iconverted": iconverted
        }
    
    return files_info


bid_index = 1
for bid in parsed_json_data:
    bid_info, bid_index = get_bid_data(bid_item=bid, bid_index=bid_index)

    if bid_info is not None:
        bid_details.update(bid_info)

with open(os.path.join(json_path, "results.json"), "w") as file:
    json.dump(bid_details, file, indent=4)

spider_record_data = extract_from_json_and_add_to_db(
    path_to_json=os.path.join(json_path, "results.json"),
    db_url=smi_data_url,
    region_name=region_name,
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

end_time = time.time()
total_execution_time = round((end_time - start_time) / 60)

session = create_database_session(database_url=smi_record_url)
insert_to_spiderrecord_database(session=session, module_name=module_name.split(".")[0], ecgains=ecgains, time_elapsed=total_execution_time,**spider_record_data)

delete_files_in_directory(download_path)
os.remove(os.path.join(json_path, "results.json"))

print("[+] End of script!")