from functools import wraps


def coroutine(func):
    """Coroutine decorator"""
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn
    return inner
