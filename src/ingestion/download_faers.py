import os
import io
import time
import zipfile
import requests


def get_manifest(manifest_url="https://api.fda.gov/download.json"):
    response = requests.get(manifest_url)
    response.raise_for_status()
    return response.json()


def list_quarter_files(manifest, quarter):
    partitions = manifest["results"]["drug"]["event"]["partitions"]
    quarter_lower = quarter.lower()
    files = []
    for p in partitions:
        if quarter_lower in p["display_name"].lower():
            files.append({
                "url": p["file"],
                "display_name": p["display_name"],
                "size_mb": float(p.get("size_mb", 0)),
                "records": int(p.get("records", 0)),
            })
    return files


def download_quarter(manifest, quarter, output_dir, skip_existing=True):
    files = list_quarter_files(manifest, quarter)
    if not files:
        print(f"No files found for quarter: {quarter}")
        return {"downloaded": 0, "skipped": 0, "errors": 0}

    quarter_dir = os.path.join(output_dir, quarter.replace(" ", ""))
    os.makedirs(quarter_dir, exist_ok=True)
    stats = {"downloaded": 0, "skipped": 0, "errors": 0, "total_bytes": 0}

    for i, file_info in enumerate(files, 1):
        url = file_info["url"]
        filename = url.split("/")[-1].replace(".json.zip", ".json")
        json_path = os.path.join(quarter_dir, filename)

        if skip_existing and os.path.exists(json_path):
            stats["skipped"] += 1
            continue

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            zip_bytes = io.BytesIO(response.content)
            with zipfile.ZipFile(zip_bytes) as zf:
                json_filename = zf.namelist()[0]
                with zf.open(json_filename) as json_file:
                    with open(json_path, "wb") as out_file:
                        out_file.write(json_file.read())
            stats["downloaded"] += 1
            stats["total_bytes"] += os.path.getsize(json_path)
        except Exception as e:
            stats["errors"] += 1
            print(f" ERROR: {e}")

    return stats
