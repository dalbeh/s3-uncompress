import json
from io import BytesIO
from os import path

from mock import patch, call, Mock
from pytest import raises
from botocore.response import StreamingBody
from s3_uncompress.uncompress import CompressedFile
from s3_uncompress.exceptions import FormatNotSupported


@patch("s3_uncompress.uncompress.b3_resource")
def test_given_bucket_and_key_when_init_compressedfile_zip_then_create_object_from_s3(
    b3_resource_mock,
):
    s3_bucket_name = "bucket-mock"
    s3_key = "key-mock/file-mock.zip"
    file_name = s3_key.split("/")[-1]

    file = CompressedFile(s3_bucket_name=s3_bucket_name, s3_key=s3_key, compressed_type="zip")

    b3_resource_mock.assert_called_once_with("s3")
    b3_resource_mock("s3").Object.assert_called_once_with(
        bucket_name=s3_bucket_name, key=s3_key
    )
    assert file.type == "zip"


@patch("s3_uncompress.uncompress.b3_resource")
def test_given_bucket_and_key_when_init_compressedfile_rar_then_create_object_from_s3(
    b3_resource_mock,
):
    s3_bucket_name = "bucket-mock"
    s3_key = "key-mock/file-mock.rar"
    file_name = s3_key.split("/")[-1]

    file = CompressedFile(s3_bucket_name=s3_bucket_name, s3_key=s3_key)

    b3_resource_mock.assert_called_once_with("s3")
    b3_resource_mock("s3").Object.assert_called_once_with(
        bucket_name=s3_bucket_name, key=s3_key
    )
    assert file.type == "rar"


@patch("s3_uncompress.uncompress.b3_resource")
def test_given_bucket_and_key_when_init_compressedfile_not_supported_then_raise_error(
    b3_resource_mock,
):
    s3_bucket_name = "bucket-mock"
    s3_key = "key-mock/file-mock.gzip"

    with raises(FormatNotSupported):
        CompressedFile(s3_bucket_name=s3_bucket_name, s3_key=s3_key)

    assert not b3_resource_mock.called


@patch("s3_uncompress.uncompress.ZipFile")
@patch("s3_uncompress.uncompress.b3_resource")
def test_given_mock_file_when_uncompress_memory_compressedfile_zip_then_exception_and_return_dict(
    b3_resource_mock, zipfile_mock
):

    s3_bucket_name = "bucket-mock"
    s3_key = "key-mock/file-mock.zip"
    s3_target_bucket_name = "uncompressed-files-mock"
    s3_target_key = "file-mock-zip"

    file = CompressedFile(s3_bucket_name=s3_bucket_name, s3_key=s3_key)

    encoded_message = json.dumps(
        {"data1_mock": "value1_mock", "data2_mock": "value2_mock"}
    ).encode("utf-8")
    raw_file_content_mock = {"Metadata": "", "Body": ""}
    raw_file_mock = StreamingBody(BytesIO(encoded_message), len(encoded_message))
    raw_file_content_mock["Body"] = raw_file_mock

    b3_resource_mock("s3").Object(
        bucket_name=s3_bucket_name, key=s3_key
    ).get.return_value = raw_file_content_mock

    zipfile_mock().namelist.return_value = ["file1_mock.jpg", "file2_mock.jpg"]
    zipfile_mock().open.return_value = "file_index_mock"

    b3_resource_mock("s3").meta.client.upload_fileobj.side_effect = Exception("error")

    response = file.uncompress_using_memory(
        s3_target_bucket=s3_target_bucket_name, s3_target_key=s3_target_key
    )

    b3_resource_mock("s3").meta.client.upload_fileobj.assert_has_calls(
        [
            call(
                "file_index_mock",
                Bucket="uncompressed-files-mock",
                Key="file-mock-zip/file1_mock.jpg",
            ),
            call(
                "file_index_mock",
                Bucket="uncompressed-files-mock",
                Key="file-mock-zip/file2_mock.jpg",
            ),
        ]
    )

    assert response == {
        "uncompress_method": "memory",
        "files": [
            {"name": "file1_mock.jpg", "status": "error"},
            {"name": "file2_mock.jpg", "status": "error"},
        ],
        "name": "file-mock.zip",
    }


@patch("s3_uncompress.uncompress.ZipFile")
@patch("s3_uncompress.uncompress.b3_resource")
def test_given_bucket_and_key_when_uncompress_memory_compressedfile_zip_then_uncompress_files_and_upload_to_s3(
    b3_resource_mock, zipfile_mock
):

    s3_bucket_name = "bucket-mock"
    s3_key = "key-mock/file-mock.zip"
    s3_target_bucket_name = "uncompressed-files-mock"
    s3_target_key = "file-mock-zip"

    file = CompressedFile(s3_bucket_name=s3_bucket_name, s3_key=s3_key)

    encoded_message = json.dumps(
        {"data1_mock": "value1_mock", "data2_mock": "value2_mock"}
    ).encode("utf-8")
    raw_file_content_mock = {"Metadata": "", "Body": ""}
    raw_file_mock = StreamingBody(BytesIO(encoded_message), len(encoded_message))
    raw_file_content_mock["Body"] = raw_file_mock

    b3_resource_mock("s3").Object(
        bucket_name=s3_bucket_name, key=s3_key
    ).get.return_value = raw_file_content_mock

    zipfile_mock().namelist.return_value = ["file1_mock.jpg", "file2_mock.jpg"]
    zipfile_mock().open.return_value = "file_index_mock"

    response = file.uncompress_using_memory(
        s3_target_bucket=s3_target_bucket_name, s3_target_key=s3_target_key
    )

    b3_resource_mock("s3").meta.client.upload_fileobj.assert_has_calls(
        [
            call(
                "file_index_mock",
                Bucket="uncompressed-files-mock",
                Key="file-mock-zip/file1_mock.jpg",
            ),
            call(
                "file_index_mock",
                Bucket="uncompressed-files-mock",
                Key="file-mock-zip/file2_mock.jpg",
            ),
        ]
    )

    assert response == {
        "uncompress_method": "memory",
        "files": [
            {"name": "file1_mock.jpg", "status": "ok"},
            {"name": "file2_mock.jpg", "status": "ok"},
        ],
        "name": "file-mock.zip",
    }


@patch("s3_uncompress.uncompress.RarFile")
@patch("s3_uncompress.uncompress.b3_resource")
def test_given_bucket_and_key_when_uncompress_memory_compressedfile_rar_then_uncompress_files_and_upload_to_s3(
    b3_resource_mock, rarfile_mock
):

    s3_bucket_name = "bucket-mock"
    s3_key = "key-mock/file-mock.rar"
    s3_target_bucket_name = "uncompressed-files-mock"
    s3_target_key = "file-mock-rar"

    file = CompressedFile(s3_bucket_name=s3_bucket_name, s3_key=s3_key)

    encoded_message = json.dumps(
        {"data1_mock": "value1_mock", "data2_mock": "value2_mock"}
    ).encode("utf-8")
    raw_file_content_mock = {"Metadata": "", "Body": ""}
    raw_file_mock = StreamingBody(BytesIO(encoded_message), len(encoded_message))
    raw_file_content_mock["Body"] = raw_file_mock

    b3_resource_mock("s3").Object(
        bucket_name=s3_bucket_name, key=s3_key
    ).get.return_value = raw_file_content_mock

    rarfile_mock().namelist.return_value = ["file1_mock.jpg", "file2_mock.jpg"]
    rarfile_mock().open.return_value = "file_index_mock"

    response = file.uncompress_using_memory(
        s3_target_bucket=s3_target_bucket_name, s3_target_key=s3_target_key
    )

    b3_resource_mock("s3").meta.client.upload_fileobj.assert_has_calls(
        [
            call(
                "file_index_mock",
                Bucket="uncompressed-files-mock",
                Key="file-mock-rar/file1_mock.jpg",
            ),
            call(
                "file_index_mock",
                Bucket="uncompressed-files-mock",
                Key="file-mock-rar/file2_mock.jpg",
            ),
        ]
    )

    assert response == {
        "uncompress_method": "memory",
        "files": [
            {"name": "file1_mock.jpg", "status": "ok"},
            {"name": "file2_mock.jpg", "status": "ok"},
        ],
        "name": "file-mock.rar",
    }


@patch("s3_uncompress.uncompress.ZipFile")
@patch("s3_uncompress.uncompress.b3_resource")
def test_given_bucket_and_key_when_uncompress_disk_compressedfile_zip_then_uncompress_files_and_upload_to_s3(
    b3_resource_mock, zipfile_mock
):

    s3_bucket_name = "bucket-mock"
    s3_key = "key-mock/file-mock.zip"
    s3_target_bucket_name = "uncompressed-files-mock"
    s3_target_key = "file-mock-zip"
    local_path_uncompress = "compressed_files"
    local_path_uncompress_object = path.dirname(f"{local_path_uncompress}/file-mock")

    file = CompressedFile(s3_bucket_name=s3_bucket_name, s3_key=s3_key)

    encoded_message = json.dumps(
        {"data1_mock": "value1_mock", "data2_mock": "value2_mock"}
    ).encode("utf-8")
    raw_file_content_mock = {"Metadata": "", "Body": ""}
    raw_file_mock = StreamingBody(BytesIO(encoded_message), len(encoded_message))
    raw_file_content_mock["Body"] = raw_file_mock

    b3_resource_mock("s3").Object(
        bucket_name=s3_bucket_name, key=s3_key
    ).get.return_value = raw_file_content_mock

    zipfile_mock().namelist.return_value = ["file1_mock.jpg", "file2_mock.jpg"]
    zipfile_mock().open.return_value = "file_index_mock"

    response = file.uncompress_using_disk(
        local_path=local_path_uncompress,
        s3_target_bucket=s3_target_bucket_name,
        s3_target_key=s3_target_key,
    )

    zipfile_mock().extract.assert_has_calls(
        [
            call(member="file1_mock.jpg", path=local_path_uncompress_object),
            call(member="file2_mock.jpg", path=local_path_uncompress_object),
        ]
    )

    b3_resource_mock("s3").Bucket(s3_target_bucket_name).upload_file.assert_has_calls(
        [
            call(
                f"{local_path_uncompress_object}/file1_mock.jpg",
                "file-mock-zip/file1_mock.jpg",
            ),
            call(
                f"{local_path_uncompress_object}/file2_mock.jpg",
                "file-mock-zip/file2_mock.jpg",
            ),
        ]
    )

    assert response == {
        "uncompress_method": "disk",
        "files": [
            {"name": "file1_mock.jpg", "status": "ok"},
            {"name": "file2_mock.jpg", "status": "ok"},
        ],
        "name": "file-mock.zip",
    }


@patch("s3_uncompress.uncompress.RarFile")
@patch("s3_uncompress.uncompress.b3_resource")
def test_given_bucket_and_key_when_uncompress_disk_compressedfile_rar_then_uncompress_files_and_upload_to_s3(
    b3_resource_mock, rarfile_mock
):

    s3_bucket_name = "bucket-mock"
    s3_key = "key-mock/file-mock.rar"
    s3_target_bucket_name = "uncompressed-files-mock"
    s3_target_key = "file-mock-rar"
    local_path_uncompress = "compressed_files"
    local_path_uncompress_object = path.dirname(f"{local_path_uncompress}/file-mock")

    file = CompressedFile(s3_bucket_name=s3_bucket_name, s3_key=s3_key)

    encoded_message = json.dumps(
        {"data1_mock": "value1_mock", "data2_mock": "value2_mock"}
    ).encode("utf-8")
    raw_file_content_mock = {"Metadata": "", "Body": ""}
    raw_file_mock = StreamingBody(BytesIO(encoded_message), len(encoded_message))
    raw_file_content_mock["Body"] = raw_file_mock

    b3_resource_mock("s3").Object(
        bucket_name=s3_bucket_name, key=s3_key
    ).get.return_value = raw_file_content_mock

    rarfile_mock().namelist.return_value = ["file1_mock.jpg", "file2_mock.jpg"]
    rarfile_mock().open.return_value = "file_index_mock"

    response = file.uncompress_using_disk(
        local_path=local_path_uncompress,
        s3_target_bucket=s3_target_bucket_name,
        s3_target_key=s3_target_key,
    )

    rarfile_mock().extract.assert_has_calls(
        [
            call(member="file1_mock.jpg", path=local_path_uncompress_object),
            call(member="file2_mock.jpg", path=local_path_uncompress_object),
        ]
    )

    b3_resource_mock("s3").Bucket(s3_target_bucket_name).upload_file.assert_has_calls(
        [
            call(
                f"{local_path_uncompress_object}/file1_mock.jpg",
                "file-mock-rar/file1_mock.jpg",
            ),
            call(
                f"{local_path_uncompress_object}/file2_mock.jpg",
                "file-mock-rar/file2_mock.jpg",
            ),
        ]
    )

    assert response == {
        "uncompress_method": "disk",
        "files": [
            {"name": "file1_mock.jpg", "status": "ok"},
            {"name": "file2_mock.jpg", "status": "ok"},
        ],
        "name": "file-mock.rar",
    }
