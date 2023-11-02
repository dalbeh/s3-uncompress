from os import path, mkdir
from shutil import rmtree
from io import BytesIO
from zipfile import ZipFile
from typing import Union

from rarfile import RarFile
from boto3 import resource as b3_resource

from s3_uncompress.exceptions import FormatNotSupported


class CompressedFile:
    def __init__(self, s3_bucket_name: str, s3_key: str, compressed_type: str = None):
        """
        Inits the CompressedFile object from an S3 file, then it creates a new object called `self.obj`,
        get the type of compression from the extension or from param and create the s3 target path

        Args:
          s3_bucket_name (str): The name of the S3 bucket where the file is located.
          s3_key (str): The path to the file in S3.
          compressed_type (str): Optional. The type of compression.
        """
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key

        if compressed_type == None:
            if s3_key.endswith(".zip"):
                self.type = "zip"
            elif s3_key.endswith(".rar"):
                self.type = "rar"
            else:
                raise FormatNotSupported({s3_key.split(".")[-1]})
        else:
            if compressed_type in ["zip", "rar", "x-rar", "x-rar-compressed", "vnd.rar"]:
                self.type = compressed_type
            else:
                raise FormatNotSupported(compressed_type)

        self.file_name = self.s3_key.split("/")[-1]
        self.file_description = {"name": self.file_name}

        self.s3_resource = b3_resource("s3")
        self.obj = self.s3_resource.Object(bucket_name=s3_bucket_name, key=s3_key)

    def __create_compressed_object(
        self, object_compressed: Union[ZipFile, RarFile]
    ) -> tuple:
        """
        It takes a bytes object compressed file or file path and returns a ZipFile/RarFile object with a
        list of the files inside it

        Args:
          object_compressed: The compressed file to be used to create the proper object.

        Returns:
          A tuple containing the object and the namelist.
        """
        if self.type == "zip":
            object = ZipFile(object_compressed, mode="r")
            namelist = [
                [x, x.encode("cp437").decode("utf8")] for x in object.namelist()
            ]
        elif "rar" in self.type:
            object = RarFile(object_compressed, mode="r")
            namelist = [[x, x] for x in object.namelist()]
        return object, namelist

    def uncompress_using_memory(
        self, s3_target_bucket: str, s3_target_key: str = None
    ) -> dict:
        """
        It uncompresses the object in memory, and then uploads the uncompressed
        files to a S3 bucket

        Args:
          s3_target_bucket (str): The bucket where you want to upload the uncompressed files.

        Returns:
          A dictionary with the following keys:
            - name (the compressed file)
            - uncompress_method
            - files (list of dict with files uncompressed and status)
        """
        self.file_description["uncompress_method"] = "memory"
        files = []
        if s3_target_key == None:
            s3_target_path = ""
        else:
            s3_target_path = s3_target_key + "/"

        self.obj_buffer = BytesIO(self.obj.get()["Body"].read())

        obj_bytes, namelist = self.__create_compressed_object(self.obj_buffer)

        for file in namelist:
            if (
                not file[0].endswith("/")
                and not file[0].startswith("__MACOSX")
                and not file[0].endswith(".DS_Store")
            ):
                try:
                    self.s3_resource.meta.client.upload_fileobj(
                        obj_bytes.open(file[0]),
                        Bucket=s3_target_bucket,
                        Key=f"{s3_target_path}{file[1].split('/')[-1]}",
                    )
                    files.append({"name": file[1], "status": "ok"})
                    self.file_description["files"] = files
                except Exception as e:
                    files.append({"name": file[1], "status": str(e)})
                    self.file_description["files"] = files

        return self.file_description

    def uncompress_using_disk(
        self, local_path: str, s3_target_bucket: str, s3_target_key: str = None
    ) -> dict:
        """
        It downloads the compressed file from S3, uncompresses it, uploads the uncompressed files to S3,
        and deletes the local files

        Args:
          local_path (str): The local path where the compressed file will be downloaded and
        uncompressed.
          s3_target_bucket (str): The bucket where you want to store the uncompressed files.

        Returns:
          A dictionary with the following keys:
            - name (the compressed file)
            - uncompress_method
            - files (list of dict with files uncompressed and status)
        """
        self.file_description["uncompress_method"] = "disk"
        files = []
        if s3_target_key == None:
            s3_target_path = ""
        else:
            s3_target_path = s3_target_key + "/"

        if path.isdir(local_path) == False:
            mkdir(local_path)
        local_path_file = f"{local_path}/{self.file_name}"
        uncompressed_file_path = path.dirname(f'{local_path}/{self.file_name.split(".")[0]}')

        bucket_target = self.s3_resource.Bucket(s3_target_bucket)
        bucket_source = self.s3_resource.Bucket(self.s3_bucket_name)
        bucket_source.download_file(self.s3_key, local_path_file)

        obj_compressed, namelist = self.__create_compressed_object(local_path_file)

        for file in namelist:
            if (
                not file[0].endswith("/")
                and not file[0].startswith("__MACOSX")
                and not file[0].endswith(".DS_Store")
            ):
                try:
                    obj_compressed.extract(member=file[0], path=uncompressed_file_path)
                    bucket_target.upload_file(
                        f"{uncompressed_file_path}/{file[0]}",
                        f"{s3_target_path}{file[1].split('/')[-1]}",
                    )
                    files.append({"name": file[1], "status": "ok"})
                    self.file_description["files"] = files
                except Exception as e:
                    files.append({"name": file[1], "status": str(e)})
                    self.file_description["files"] = files

        rmtree(local_path)
        return self.file_description
