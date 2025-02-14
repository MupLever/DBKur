from pprint import pprint


def logger(func):
    def wrapper(*args, **kwargs):
        print("#" * 90)
        print("REQUEST: ")
        result = func(*args, **kwargs)
        pprint(result)
        print("#" * 90)
        return result

    return wrapper
