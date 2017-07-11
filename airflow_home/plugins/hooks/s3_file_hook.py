import re
import fnmatch

from airflow.hooks.S3_hook import S3Hook

exclude_dirs = lambda paths: [i for i in paths if not i.endswith('/')]


class S3FileHook(S3Hook):
    """
    Similar to built-in S3Hook except excludes directories and supports multiple keys.
    """

    def get_wildcard_key(self, *args, **kwargs):
        bucket, key_matches = self.get_wildcard_keys(*args, **kwargs)
        return bucket.get_key(key_matches[0]) if key_matches else None

    def get_wildcard_keys(self, wildcard_key, bucket_name=None, delimiter=''):
        """
        Like get_wildcard_key except returns all matches instead of the first.
        """
        if not bucket_name:
            (bucket_name, wildcard_key) = self.parse_s3_url(wildcard_key)
        bucket = self.get_bucket(bucket_name)
        prefix = re.split(r'[*]', wildcard_key, 1)[0]
        klist = self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        if not klist:
            return None, None
        key_matches = [k for k in klist if fnmatch.fnmatch(k, wildcard_key)]
        # prevent "directories" from returning in results as we only
        # want to match files (not an empty top-level directory)
        key_matches = exclude_dirs(key_matches)
        return bucket, key_matches

    def delete_s3_key(self, key, bucket_name):
        """Removes S3 Key"""
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)
        bucket = self.get_bucket(bucket_name)
        return bucket.delete_key(key)
