from abc import ABCMeta, abstractmethod


class DumpBase(metaclass=ABCMeta):
    def __init__(name=None, config=None, **kwargs):
        pass

    @property
    def label(self):
        return self.__class__.__name__

    def __repr__(self):
        return self.label

    @abstractmethod
    def dumps(self):
        return NotImplemented

    @abstractmethod
    def loads(self, sql=None):
        return NotImplemented
