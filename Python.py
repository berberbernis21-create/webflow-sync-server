import os
import json
import hashlib
import mimetypes
import requests

WEBFLOW_TOKEN = "1748644312288ac0d26cc6925b2476809a55786de20804d457212344a66ae047"
SITE_ID = "5e8d436ca3f96345b47da055"

IMAGE_PATHS = [
    r"C:\Users\bberb\Downloads\four hands table.jpg",
    r"C:\Users\bberb\Downloads\four hands tbale 2.jpg",
    r"C:\Users\bberb\Downloads\Iron Glass 3.jpg",
    r"C:\Users\bberb\Downloads\Iron Glas 2.jpg",
]

def sha1_file(path):
    sha1 = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            sha1.update(chunk)
    return sha1.hexdigest()

def guess_content_type(path):
    ctype, _ = mimetypes.guess_type(path)
    return ctype or "application/octet-stream"

def create_webflow_asset(file_path):
    file_name = os.path.basename(file_path)
    file_hash = sha1_file(file_path)

    url = f"https://api.webflow.com/v2/sites/{SITE_ID}/assets"
    headers = {
        "Authorization": f"Bearer {WEBFLOW_TOKEN}",
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    payload = {
        "fileName": file_name,
        "fileHash": file_hash
    }

    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    print("\nCREATE ASSET STATUS:", resp.status_code)
    resp.raise_for_status()
    return resp.json()

def upload_file_to_s3(upload_data, file_path):
    upload_url = upload_data["uploadUrl"]
    details = upload_data["uploadDetails"]

    form_data = {k: v for k, v in details.items()}
    content_type = upload_data.get("contentType") or guess_content_type(file_path)

    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f, content_type)}
        resp = requests.post(upload_url, data=form_data, files=files)
        print("UPLOAD TO S3 STATUS:", resp.status_code)
        resp.raise_for_status()

def main():
    results = []

    for path in IMAGE_PATHS:
        if not os.path.exists(path):
            print("❌ FILE NOT FOUND:", path)
            continue

        print("\n===================================")
        print("Uploading:", path)
        print("===================================")

        asset_data = create_webflow_asset(path)
        upload_file_to_s3(asset_data, path)

        results.append({
            "file": os.path.basename(path),
            "assetId": asset_data["id"],
            "hostedUrl": asset_data["hostedUrl"],
        })

    print("\n\n✅ DONE. HOSTED URLS:")
    for r in results:
        print(f"- {r['file']} -> {r['hostedUrl']}")

if __name__ == "__main__":
    main()
