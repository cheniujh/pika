import os
import sys
import requests
import zlib

def compress_file(file_path):
    """
    使用zlib将文件压缩为gzip格式，返回压缩后的文件路径。
    """
    compressed_file_path = f"{file_path}.gz"
    with open(file_path, 'rb') as f_in, open(compressed_file_path, 'wb') as f_out:
        compressor = zlib.compressobj(wbits=zlib.MAX_WBITS | 16)  # gzip压缩
        chunk = f_in.read(1024 * 1024)  # 每次读取1MB
        while chunk:
            f_out.write(compressor.compress(chunk))
            chunk = f_in.read(1024 * 1024)
        f_out.write(compressor.flush())
    return compressed_file_path

def upload_artifact(file_path, artifact_name, token, run_id):
    """
    将压缩文件作为GitHub Artifact上传
    """
    compressed_file_path = compress_file(file_path)  # 生成gzip压缩文件
    file_size = os.path.getsize(compressed_file_path)

    upload_url = f"https://uploads.github.com/repos/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{run_id}/artifacts"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # 初始化上传请求，获取用于上传的URL
    data = {
        "name": artifact_name,
        "size": file_size  # 指定压缩后文件的大小
    }
    response = requests.post(upload_url, headers=headers, json=data)
    if response.status_code != 201:
        print(f"Error initializing upload: {response.json()}")
        return

    # 从初始化响应中获取实际的上传URL
    upload_response = response.json()
    blob_upload_url = upload_response['url']

    # 一次性上传整个压缩文件
    with open(compressed_file_path, 'rb') as f:
        compressed_data = f.read()
        upload_headers = {
            **headers,
            "Content-Length": str(file_size),
            "Content-Type": "application/octet-stream",
        }
        upload_response = requests.put(blob_upload_url, headers=upload_headers, data=compressed_data)

        if upload_response.status_code != 200:
            print(f"Error uploading artifact: {upload_response.json()}")
            return

    print(f"Uploaded {artifact_name} successfully with compressed size {file_size} bytes")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: upload_artifact.py <file_path> <artifact_name>")
        sys.exit(1)

    file_path = sys.argv[1]
    artifact_name = sys.argv[2]
    run_id = os.getenv("GITHUB_RUN_ID")
    token = os.getenv("GITHUB_TOKEN")

    upload_artifact(file_path, artifact_name, token, run_id)
