import threading
from typing import Any


class Singleton:
    _instance: Any | None = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance



# class A(Singleton):
#     pass
# class B(Singleton):
#     pass
#
# # a = Singleton()
# # b = Singleton()
# #
# # print(a is b)
# # print(id(a))
# # print(id(b))
#
# a = A()
# b = B()
#
# print(a is b)
# print(id(a))
# print(id(b))