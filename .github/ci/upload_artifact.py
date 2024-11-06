import os
import sys
import requests

def upload_artifact(file_path, artifact_name, token, run_id):
    """
    将文件作为artifact上传到指定的GitHub workflow运行。
    """
    # 打开文件并读取内容
    with open(file_path, "rb") as f:
        file_data = f.read()

    # GitHub API上传Artifact的URL
    upload_url = f"https://uploads.github.com/repos/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{run_id}/artifacts"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/zip",
        "Accept": "application/vnd.github.v3+json",
    }

    # 参数定义，包含artifact名称和大小
    params = {
        "name": artifact_name,
        "size": len(file_data)
    }

    # 创建artifact的请求
    response = requests.post(upload_url, headers=headers, json=params)
    if response.status_code != 201:
        print(f"Error creating artifact: {response.json()}")
        return

    # 获取上传的blob URL
    artifact_data = response.json()
    blob_upload_url = artifact_data['url']

    # 上传文件数据到blob URL
    response = requests.put(blob_upload_url, headers=headers, data=file_data)
    if response.status_code != 200:
        print(f"Error uploading file: {response.json()}")
    else:
        print(f"Uploaded {artifact_name} successfully")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: upload_artifact.py <file_path> <artifact_name>")
        sys.exit(1)

    # 从命令行参数中获取文件路径和artifact名称
    file_path = sys.argv[1]
    artifact_name = sys.argv[2]

    # 从环境变量中获取GitHub相关信息
    run_id = os.getenv("GITHUB_RUN_ID")         # 当前workflow运行ID
    token = os.getenv("GITHUB_TOKEN")           # GitHub Token，用于认证

    upload_artifact(file_path, artifact_name, token, run_id)
