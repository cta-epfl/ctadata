
def urljoin_multipart(*args):
    """Join multiple parts of a URL together, ignoring empty parts."""
    return "/".join([arg.strip("/") for arg in args if arg is not None and arg.strip("/") != ""])