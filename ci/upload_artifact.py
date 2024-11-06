import os
import sys
import requests

def upload_artifact(file_path, artifact_name, token, run_id):
    """
    Upload a file as an artifact to a specific GitHub workflow run.
    """
    # 打开文件并读取内容
    with open(file_path, "rb") as f:
        file_data = f.read()

    # GitHub API上传Artifact的URL
    upload_url = f"https://uploads.github.com/repos/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{run_id}/artifacts"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # 使用`multipart/form-data`格式进行请求
    files = {
        'file': (artifact_name, file_data)
    }
    data = {
        "name": artifact_name
    }

    # 发送上传请求
    response = requests.post(upload_url, headers=headers, data=data, files=files)
    if response.status_code != 201:
        print(f"Error creating artifact: {response.json()}")
    else:
        print(f"Uploaded {artifact_name} successfully")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: upload_artifact.py <file_path> <artifact_name>")
        sys.exit(1)

    file_path = sys.argv[1]
    artifact_name = sys.argv[2]
    run_id = os.getenv("GITHUB_RUN_ID")
    token = os.getenv("GITHUB_TOKEN")

    upload_artifact(file_path, artifact_name, token, run_id)
