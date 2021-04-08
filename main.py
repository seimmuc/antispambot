import json
import os
import sqlite3
import time
from typing import List, Dict, Any, Tuple, Optional

from discord import Client, Status, Member, Message, Intents, Guild, TextChannel, ChannelType, PartialMessage


class ActivityLimit:
    def __init__(self, time_window: float, count_limit: int):
        self.time_window = time_window
        self.count_limit = count_limit


class Record:
    def __init__(self, timestamp: float, action_name: str, initiator: str = None, data=None):
        self.timestamp = timestamp
        self.action_name = action_name
        self.initiator = initiator
        self.data = data


class RecentActivity:
    def __init__(self, db_con: sqlite3.Connection, table_name: str, limit: ActivityLimit):
        self.db_con = db_con
        self.table_name = table_name
        self.activity_limit = limit
        self._last_fetched_times = {}

    @staticmethod
    def _sql_where_clause(conditions: Dict[str, Tuple[str, Any]]) -> Tuple[str, List[Any]]:
        # conditions: Dict[column_name, Tuple[condition_operator, comparison_value]]
        where_clause, where_values = zip(*((f'"{k}" {v[0]} ?', v[1]) for k, v in conditions.items()))
        where_clause = 'WHERE ' + ' AND '.join(where_clause)
        return where_clause, where_values

    def add_record(self, guild_id: int, **data):
        names = ['guild_id', 'unix_time']
        values = [guild_id, time.time()]
        for k, v in data.items():
            names.append(f'"{k}"')
            values.append(v)
        with self.db_con:
            self.db_con.execute(
                    f'INSERT INTO {self.table_name} ({", ".join(names)}) VALUES ({", ".join("?" for _ in names)})',
                    values)

    def purge_old(self):
        timestamp = time.time() - self.activity_limit.time_window
        with self.db_con:
            self.db_con.execute(f'DELETE FROM {self.table_name} WHERE unix_time < ?', [timestamp])

    def over_limit(self, guild_id: int, purge: bool, **match_conditions) -> bool:
        if purge:
            self.purge_old()
        match_conditions['guild_id'] = guild_id
        where_clause, where_values = self._sql_where_clause({k: ('=', v) for k, v in match_conditions.items()})
        with self.db_con as con:
            c = con.execute(f'SELECT COUNT (unix_time) FROM {self.table_name} {where_clause}', where_values)
            row_count = c.fetchone()[0]
        return row_count > self.activity_limit.count_limit

    def fetch_recent_records_once(self, guild_id: int, purge: bool, columns: List[str] = None, **match_conditions)\
            -> list:
        if purge:
            self.purge_old()
        match_conditions['guild_id'] = guild_id

        # this is not thread safe, but is fine with asyncio
        last_fetched_key = f'{self.table_name}:{"|".join(f"{k}={v}" for k,v in match_conditions.items())}'
        last_fetch_time = self._last_fetched_times.get(last_fetched_key, 0)
        self._last_fetched_times[last_fetched_key] = time.time()

        conditions = {k: ('=', v) for k, v in match_conditions.items()}
        conditions['unix_time'] = ('>', last_fetch_time)
        where_clause, where_values = self._sql_where_clause(conditions)
        columns = '*' if columns is None else ', '.join(f'"{c}"' for c in columns)
        with self.db_con as con:
            c = con.execute(f'SELECT {columns} FROM {self.table_name} {where_clause}', where_values)
            return c.fetchall()


class AntiSpamBot(Client):
    def __init__(self, config: Dict[str, Any], db_con: sqlite3.Connection, *args, **kwargs):
        intents = Intents(members=True, messages=True, guilds=True)
        super().__init__(intents=intents, *args, **kwargs)
        self.db_con = db_con
        self.enable_message_limit = config.get('ENABLE_MESSAGE_LIMIT', None) is True
        self.message_activity = RecentActivity(db_con=db_con, table_name='Message',
                                               limit=ActivityLimit(**config['MESSAGE_LIMIT']))
        self.join_activity = RecentActivity(db_con=db_con, table_name='GuildJoin',
                                            limit=ActivityLimit(**config['JOIN_LIMIT']))

    async def on_ready(self):
        print(f'We have logged in as {self.user}')

    async def on_message(self, message: Message):
        if not self.enable_message_limit:
            return
        if message.author.id == self.user.id:
            return
        if message.channel.type != ChannelType.text:
            return

        guild: Guild = message.guild
        user_id = message.author.id
        self.message_activity.add_record(guild_id=guild.id, user_id=user_id, channel_id=message.channel.id,
                                         message_id=message.id)
        if self.message_activity.over_limit(guild_id=guild.id, purge=True, user_id=user_id):
            print('hit the text message limit')
            await message.channel.send(content='hit the text message limit')
            del_msgs: List[sqlite3.Row] = self.message_activity\
                .fetch_recent_records_once(guild_id=guild.id, purge=False, columns=['channel_id', 'message_id'],
                                           user_id=user_id)
            if len(del_msgs) == 1:
                msg = guild.get_channel(del_msgs[0]['channel_id']).get_partial_message(del_msgs[0]['message_id'])
                await msg.delete()
            elif len(del_msgs) > 1:
                del_msgs_by_channel: Dict[int, Tuple[TextChannel, List[PartialMessage]]] =\
                    {ci: (guild.get_channel(ci), []) for ci in set(m['channel_id'] for m in del_msgs)}
                for msg in del_msgs:
                    ch, ml = del_msgs_by_channel[msg['channel_id']]
                    ml.append(ch.get_partial_message(msg['message_id']))
                for channel, messages in del_msgs_by_channel.values():
                    await channel.delete_messages(messages)

    async def on_member_join(self, member: Member):
        print(f'{member.name} joined')
        guild: Guild = member.guild
        self.join_activity.add_record(guild_id=guild.id, user_id=member.id)
        if self.join_activity.over_limit(guild_id=guild.id, purge=True):
            print('hit the join limit')
            kick_users: List[sqlite3.Row] = self.join_activity\
                .fetch_recent_records_once(guild_id=guild.id, purge=False, columns=['user_id'])
            for user_row in kick_users:
                user: Optional[Member] = member if user_row['user_id'] == member.id \
                    else guild.get_member(user_row['user_id'])
                if user is not None:
                    await user.kick(reason='raid protection')

    async def on_member_remove(self, member: Member):
        print(f'{member.name} left')


def init_db(db_con: sqlite3.Connection):
    init_script = """
    CREATE TABLE Message (guild_id integer, unix_time real, user_id integer, channel_id integer, message_id integer);
    CREATE TABLE GuildJoin (guild_id integer, unix_time real, user_id integer);
    """
    with db_con:
        db_con.executescript(init_script)


def start_bot(config, db_con):
    bot_token = os.getenv('ANTISPAMBOT_TOKEN')
    if bot_token is None:
        print('missing ANTISPAMBOT_TOKEN environment variable')
        raise NoTokenError()
    bot = AntiSpamBot(config=config, db_con=db_con, status=Status.idle)
    bot.http.user_agent = 'AntiSpamBot/0.1'
    bot.run(bot_token, bot=True)


class NoTokenError(RuntimeError):
    pass


if __name__ == '__main__':
    with open('config.json', 'rt', encoding='utf8') as f:
        bot_config = json.load(f)
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_db(db_con=db)
    try:
        start_bot(bot_config, db)
    except NoTokenError:
        exit(1)
