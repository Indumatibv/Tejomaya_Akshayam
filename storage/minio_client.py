# storage/minio_client.py
import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
from minio.deleteobjects import DeleteObject

load_dotenv()

class MinIOClient:
    def __init__(self):
        self.client = Minio(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=os.getenv("MINIO_SECURE", "False").lower() == "true"
        )
        self.bucket = os.getenv("MINIO_BUCKET_NAME")

        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def upload_file(self, local_path: str, object_path: str):
        self.client.fput_object(
            bucket_name=self.bucket,
            object_name=object_path,
            file_path=local_path
        )

    def upload_folder(self, local_folder: str, prefix: str):
        """
        Uploads entire folder recursively to MinIO
        """
        for root, _, files in os.walk(local_folder):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_folder)
                object_path = f"{prefix}/{relative_path}"

                self.upload_file(local_file, object_path)

    def delete_prefix(self, prefix: str):
        objects = self.client.list_objects(
            self.bucket,
            prefix=prefix,
            recursive=True
        )

        delete_objects = (DeleteObject(obj.object_name) for obj in objects)

        # 🔥 MUST consume the iterator
        errors = self.client.remove_objects(self.bucket, delete_objects)
        for error in errors:
            raise RuntimeError(
                f"MinIO delete failed: {error.object_name} → {error.message}"
            )
