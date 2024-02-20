class S3ConnectionError(Exception):
    """ Exception for S3 connection errors """
    pass


class TagsNotMatchError(Exception):
    """ Exception when two tags aren't equals """
    pass
