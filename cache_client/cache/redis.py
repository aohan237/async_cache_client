import aioredis
import time
from aioredis.commands.sorted_set import SortedSetCommandsMixin
from .base import CacheBase
from ..dump_tool.yaml_tool import YamlDump
import logging
logger = logging.getLogger(__package__)


class RedisCache(CacheBase):
    def __init__(
            self, conf=None, cached_set_postfix=None,
            cached_control_postfix=None,
            cached_key=None, dump_tool=None, redis_peak=None):

        dump_tool = dump_tool or YamlDump
        self.dump_tool = dump_tool()
        self.redis_host = conf.get('redis_host')
        self.redis_port = conf.get('redis_port')
        self.redis_password = conf.get('redis_password')

        self.cached_set_postfix = cached_set_postfix or ':zset'
        self.cached_control_postfix = cached_control_postfix or ':control'
        self._cached_key = cached_key or 'RedisCache'
        self.peak = redis_peak or 80

    async def connect(self, loop=None):
        create_pool = aioredis.create_redis_pool(
            (self.redis_host, self.redis_port),
            password=self.redis_password,
            minsize=5,
            maxsize=10,
            loop=loop
        )
        self.pool = await create_pool

    @property
    async def valid(self):
        info = await self.pool.info('memory')
        try:
            memory = info['memory']
            check = float(memory['used_memory']) / \
                float(memory['total_system_memory']) * 100
        except Exception as tmp:
            logger.exception(tmp)
            check = 100
        if check < self.peak:
            return True
        else:
            return False

    async def set_control_info(
            self, database=None, set_time=None, key=None,
            update_interval=None):
        database = database + self.cached_control_postfix
        data = {
            'set_time': set_time,
            'update_interval': update_interval
        }
        value = self.dump_tool.dumps(data)
        await self.pool.hset(database, key, value)

    async def get_control_info(
            self, database=None,  key=None):
        database = database + self.cached_control_postfix
        data = await self.pool.hget(database, key)
        if data:
            value = self.dump_tool.loads(data)
        else:
            value = {}
        return value

    async def should_update(self, database=None, key=None):
        now = int(time.time())
        value = await self.get_control_info(database=database, key=key)
        if value:
            set_time = value.get('set_time', 0)
            update_interval = value.get('update_interval', 0)
            logger.info(
                f'should update decide:{now},{set_time + update_interval}')
            return now >= set_time + update_interval
        return False

    async def add_to_regulate_set(
        self, database=None, set_time=None, key=None,
            update_interval=None, expire_at=None):
        database = database + self.cached_set_postfix
        # 添加记录的数据库名称
        await self.pool.sadd(self._cached_key, database)
        # 记录每一个数据对应的缓存数据，如果过期则删除
        exist = await self.pool.zscore(database, key)
        if exist:
            if expire_at:
                await self.pool.zadd(database, expire_at, key)
            return True
        else:
            expire_at = expire_at or int(time.time()) + 300
            await self.pool.zadd(database, expire_at, key)
            return True

    async def set(
            self, database=None, key=None, value=None,
            expire_at=None, update_interval=None, set_time=None):
        value = self.dump_tool.dumps(data=value)
        await self.set_control_info(
            database=database, set_time=set_time, key=key,
            update_interval=update_interval)
        await self.add_to_regulate_set(
            database=database, key=key,
            update_interval=update_interval,
            expire_at=expire_at, set_time=set_time,)
        return await self.pool.hset(database, key, value)

    async def exist(self, database=None, key=None):
        return await self.pool.hexists(database, key)

    async def get(self, database=None, key=None):
        result_byte = await self.pool.hget(database, key)
        try:
            result = self.dump_tool.loads(result_byte)
        except Exception as tmp:
            logger.exception(tmp)
            result = result_byte.decode() if result_byte else {}
        return result

    async def get_update_data(self):
        now = int(time.time())
        all_databases = await self.pool.smembers(self._cached_key)
        all_databases = {i.decode() for i in all_databases}
        result = {}
        for database in all_databases:
            expire_list = await self.pool.zrangebyscore(
                database, float('-inf'), now,
                exclude=SortedSetCommandsMixin.ZSET_EXCLUDE_MAX)
            expire_list = [i.decode() for i in expire_list]
            # 从数据库种删除对应的sql缓存内容
            await self.pool.zrem(database, '', *expire_list)
            # 从控制数据库中删除对应的控制信息
            control_database, _ = database.split(':')
            control_database = control_database + self.cached_control_postfix
            await self.pool.hdel(control_database, '', *expire_list)
            # 从数据缓存中删除对应的内容
            body_database, _ = database.split(':')
            await self.pool.hdel(body_database, '', *expire_list)

            update_list = await self.pool.zrangebyscore(
                database, now, float('+inf'))
            update_list = [i.decode() for i in update_list]
            database, _ = database.split(':')
            # 过滤语句级 更新间隔，间隔较长的内容将不做更新
            filtered_update_list = []
            for i in update_list:
                update = await self.should_update(database, i)
                if update:
                    filtered_update_list.append(i)
            if filtered_update_list:
                result[database] = filtered_update_list
        return result
