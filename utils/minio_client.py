## I need this because 
   ## to upload multiple files (e.g., PDF reports) to the MinIO bucket .
   ## to automate this task so you don’t manually upload files through the MinIO web interface.


from minio import Minio

def get_minio_client(endpoint, access_key, secret_key):
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False 
    )

