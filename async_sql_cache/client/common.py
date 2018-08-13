import asyncio
import yaml
from ..cache.redis import RedisCache
from ..sql.mysql import MysqlClient
from .base import ClientBase


class SimpleClient(ClientBase):
    def __init__(
        self, mysql_conf=None,
        redis_conf=None, update_interval=None, loop=None,
            **kwargs):
        self.sql = MysqlClient(conf=mysql_conf, **kwargs)
        self.cache = RedisCache(conf=redis_conf, **kwargs)
        self.update_interval = update_interval or 300
        self.loop = loop or asyncio.get_event_loop()

    async def connect(self):
        await self.sql.connect()
        await self.cache.connect()
        self.loop.create_task(self.init_period_auto_update())

    async def set(self, database=None, key=None, expire_at=None):
        result = await self.sql.get(key)
        result1 = yaml.dump(result)
        cache_valid = await self.cache.valid
        if cache_valid:
            await self.cache.set(
                database=database, key=key,
                expire_at=expire_at, value=result1)
        return result

    async def get(self, database=None, key=None, expire_at=None):
        if await self.cache.exist(database=database, key=key):
            if expire_at:
                await self.cache.add_to_regulate_set(
                    database=database,
                    key=key, expire_at=expire_at)
            return await self.cache.get(database=database, key=key)
        else:
            return await self.set(
                database=database, key=key, expire_at=expire_at)

    async def init_period_auto_update(self):
        print('auto update task')
        while True:
            update_data = await self.cache.get_update_data()
            print(update_data, self.update_interval)
            for database, sql_list in update_data.items():
                await asyncio.gather(*[
                    self.set(database=database, key=sql) for sql in sql_list])
            await asyncio.sleep(self.update_interval)
