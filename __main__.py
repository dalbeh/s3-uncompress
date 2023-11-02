from s3_uncompress import uncompress

file = uncompress.CompressedFile(s3_bucket_name='bucket', s3_key='key/file.rar',compressed_type='x-rar')
#file = uncompress.CompressedFile(s3_bucket_name='cep-dev-unstructured-eng', s3_key='inspect/images_sub.gzip')

# result = file.uncompress_using_memory(s3_target_bucket='unzip-test-123', s3_target_key='carpeta_generica')
result = file.uncompress_using_disk(local_path='compressed_files', s3_target_bucket='unzip-test-123', s3_target_key='carpeta_generica')

print(result)