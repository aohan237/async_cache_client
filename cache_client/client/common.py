import asyncio
import time
from ..cache.redis import RedisCache
from ..backend.mysql import MysqlBackend
from .base import ClientBase
import logging
logger = logging.getLogger(__package__)


class SimpleClient(ClientBase):
    def __init__(
        self, backend_conf=None,
        cache_conf=None, update_interval=None,
            **kwargs):
        self.backend = MysqlBackend(conf=backend_conf, **kwargs)
        self.cache = RedisCache(conf=cache_conf, **kwargs)
        self.update_interval = update_interval or 300

    async def connect(self, loop=None):
        loop = loop or asyncio.get_event_loop()
        await self.backend.connect(loop=loop)
        await self.cache.connect(loop=loop)
        loop.create_task(self.init_period_auto_update())

    async def set(self, database=None, key=None,
                  expire_at=None, update_interval=None):
        result = await self.backend.get(key)
        cache_valid = await self.cache.valid
        if cache_valid:
            if update_interval is None:
                update_interval_data = await self.cache.get_control_info(
                    database=database, key=key)
                update_interval = update_interval_data.get(
                    update_interval, None)
                if update_interval is None:
                    update_interval = self.update_interval
            set_time = int(time.time())
            await self.cache.set(
                database=database, key=key, set_time=set_time,
                expire_at=expire_at, value=result,
                update_interval=update_interval)
        return result

    async def get(self, database=None, key=None, force_update=False,
                  expire_at=None, update_interval=None):
        if await self.cache.exist(database=database, key=key) and \
                not force_update:
            if expire_at:
                await self.cache.add_to_regulate_set(
                    database=database,
                    key=key, expire_at=expire_at)
            return await self.cache.get(database=database, key=key)
        else:
            return await self.set(
                database=database, key=key, expire_at=expire_at,
                update_interval=update_interval)

    async def init_period_auto_update(self):
        logger.debug('auto update task')
        while True:
            update_data = await self.cache.get_update_data()
            logger.debug(f'{update_data}, {self.update_interval}')
            for database, sql_list in update_data.items():
                await asyncio.gather(*[
                    self.set(database=database, key=sql) for sql in sql_list])
            await asyncio.sleep(self.update_interval)
