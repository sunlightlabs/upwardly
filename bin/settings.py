import os

BIN_DIR = os.path.abspath(os.path.dirname(__file__))
PROJ_DIR = os.path.abspath(os.path.join(BIN_DIR, '..'))
DATA_DIR = os.path.abspath(os.path.join(BIN_DIR, '..', 'data'))

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DATABASE = 'k2'

MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASS = ''
MYSQL_DATABASE = 'k2'

def dataset_path(dataset, filename=None, mkdir=True):
    if not dataset:
        path = DATA_DIR
    else:
        path = os.path.join(DATA_DIR, dataset)
    if mkdir and not os.path.exists(path):
        os.mkdir(path)
    if filename:
        path = os.path.join(path, filename)
    return path
