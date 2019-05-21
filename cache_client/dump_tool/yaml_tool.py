from .base import DumpBase
import yaml
import logging

logger = logging.getLogger(__package__)


class YamlDump(DumpBase):
    def __init__(self):
        self.tool = yaml

    def dumps(self, data=None):
        try:
            res = self.tool.dump(data)
        except Exception as tmp:
            logger.exception(tmp)
            res = None
        return res

    def loads(self, data=None):
        try:
            res = self.tool.load(data)
        except Exception as tmp:
            logger.exception(tmp)
            res = None
        return res
