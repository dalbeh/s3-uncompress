# s3-uncompress
This library is used to uncompress files that are in s3. It creates a CompressedFile object wich have two methods with extraction capabilities using memory or disk.

### Object creation example
If the file name contains the extension, the compression type will be get from there, if not, the compressed_type parameter needs to be setted (Ex: "zip", "rar", "x-rar", "x-rar-compressed", "vnd.rar").

`CompressedFile(s3_bucket_name='example-source-bucket', s3_key='example.zip', compressed_type=None)`

### uncompress_using_memory
Reads the compressed object directly from an S3 bucket and loads the content in bytes over memory. Then iterates to extract each file to the defined S3 destination.

`uncompress_using_memory(s3_target_bucket='example-target-bucket', s3_target_key='example/target/key')`

### uncompress_using_disk
Downloads the compressed object from an S3 bucket to a local path. Then iterates the content to extract each file to the same local path, and finally upload all files to the defined S3 destination and delete local_path.

`uncompress_using_disk(local_path='example_local_path', s3_target_bucket='example-target-bucket', s3_target_key='example/target/key')`

## Supported Formats
* #####  **zip**
* ##### **rar**