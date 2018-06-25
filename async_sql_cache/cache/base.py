from abc import ABCMeta, abstractmethod


class CacheBase(metaclass=ABCMeta):
    def __init__(name=None, config=None, **kwargs):
        pass

    @property
    def label(self):
        return self.__class__.__name__

    def __repr__(self):
        return self.label

    @abstractmethod
    def connect(self):
        return NotImplemented

    @abstractmethod
    def set(self):
        return NotImplemented

    @abstractmethod
    def get(self):
        return NotImplemented

    @abstractmethod
    def exist(self):
        return NotImplemented
