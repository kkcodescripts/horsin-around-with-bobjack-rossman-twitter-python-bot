import boto3


class S3Helper:
    """
    Args: None
    """

    def __init__(self, ):
        self.client = boto3.client("s3")


    def s3_list(self, bucket_name, s3_dir):
        # Create a reusable Paginator
        paginator = self.client.get_paginator('list_objects_v2')

        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=s3_dir)
        bucket_object_list = []
        for page in page_iterator:
            if "Contents" in page:
                for key in page[ "Contents" ]:
                    keyString = key[ "Key" ]
                    bucket_object_list.append(keyString)
        
        return bucket_object_list


    def download(self, bucket, key, filename):
        try:
            self.client.download_file(bucket, key, filename)
        except Exception as e:
            raise e
        


