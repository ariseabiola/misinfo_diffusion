def format_query(s):
    pass


def read_keywords_from_file(filename=None):
    queries = ''
    with open(filename) as f:
        queries = f.readlines()
    return queries
