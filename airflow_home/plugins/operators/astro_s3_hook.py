import re
import fnmatch

from airflow.hooks import S3Hook

exclude_dirs = lambda paths: [i for i in paths if not i.endswith('/')]


# TODO: give this more relevant name
class AstroS3Hook(S3Hook):
    """
    Similar to the built-in S3Hook except this does not match directories.
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
            return None
        key_matches = [k for k in klist if fnmatch.fnmatch(k, wildcard_key)]
        # prevent "directories" from returning in results as we only
        # want to match files (not an empty top-level directory)
        key_matches = exclude_dirs(key_matches)
        return bucket, key_matches
