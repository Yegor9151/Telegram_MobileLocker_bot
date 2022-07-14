import os


def open_file(path, mode="r", encoding="utf-8", text=None):
    with open(path, mode=mode, encoding=encoding) as file:
        if mode == "r":
            return file.read()
        elif mode == "w":
            file.write(text)


def create_dir(path):
    try:
        os.mkdir(path)
        return True
    except FileExistsError as e:
        print(e)
        return False