import os


def get_data_path(fn):
    directory = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(directory, 'data', fn)
