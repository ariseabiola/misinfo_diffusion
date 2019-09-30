def format_query(s):
    pass


def read_keywords_file(filename=None):
    query = ''
    with open(filename) as f:
        query = f.read()
    return query
