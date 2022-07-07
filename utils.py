import os


def read_file(path, encoding="utf-8"):
    with open(path, mode="r", encoding=encoding) as file:
        return file.read()


def create_dir(path):
    try:
        os.mkdir(path)
        return True
    except FileExistsError as e:
        print(e)
        return False