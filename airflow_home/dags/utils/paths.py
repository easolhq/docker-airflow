def ensure_trailing_slash(path):
    """
    Ensure directory paths include a trailing slash.
    """
    if not path.endswith('/'):
        path += '/'
    return path
