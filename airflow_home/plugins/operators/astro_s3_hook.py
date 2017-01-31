import re
import fnmatch

from airflow.hooks import S3Hook


class AstroS3Hook(S3Hook):
    """
    Similar to the built-in S3Hook except this does not match directories.
    """

    def get_wildcard_key(self, wildcard_key, bucket_name=None, delimiter=''):
        if not bucket_name:
            (bucket_name, wildcard_key) = self.parse_s3_url(wildcard_key)
        bucket = self.get_bucket(bucket_name)
        prefix = re.split(r'[*]', wildcard_key, 1)[0]
        klist = self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        if not klist:
            return None
        key_matches = [k for k in klist if fnmatch.fnmatch(k, wildcard_key)]
        # prevent "directories" from returning in results as we only
        # want to match files (not an empty top-level directory)
        key_matches = [i for i in key_matches if not i.endswith('/')]
        return bucket.get_key(key_matches[0]) if key_matches else None
