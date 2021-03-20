import json
import os
import time
import warnings
from typing import List, Dict, Any

from discord import Client, Status, Member, Message, Intents


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
    def __init__(self, action_name: str):
        self.action_name = action_name
        self.activity: List[Record] = []

    def add_record(self, action_name=None, initiator=None, action_data=None):
        if action_name is None:
            action_name = self.action_name
        record = Record(timestamp=time.time(), action_name=action_name, initiator=initiator, data=action_data)
        self.activity.append(record)

    def purge_before(self, time_diff: float):
        # Note: This is not thread-safe, but we're only dealing with async here. Everything should run in main thread.
        timestamp = time.time() - time_diff
        self.activity = [r for r in self.activity if r.timestamp > timestamp]

    def over_limit(self, limit: ActivityLimit) -> bool:
        self.purge_before(time_diff=limit.time_window)
        return len(self.activity) > limit.count_limit


class AntiSpamBot(Client):
    def __init__(self, config: Dict[str, Any], *args, **kwargs):
        intents = Intents(members=True, messages=True, guilds=True)
        super().__init__(intents=intents, *args, **kwargs)
        self.enable_message_limit = config.get('ENABLE_MESSAGE_LIMIT', None) is True
        self.message_activity = RecentActivity(action_name='messages')
        self.message_limit = ActivityLimit(**config['MESSAGE_LIMIT'])
        self.join_activity = RecentActivity(action_name='join')
        self.join_limit = ActivityLimit(**config['JOIN_LIMIT'])

    async def on_ready(self):
        print(f'We have logged in as {self.user}')

    async def on_message(self, message: Message):
        if message.author.id == self.user.id:
            return
        if not self.enable_message_limit:
            return
        self.message_activity.add_record(initiator=message.author.id, action_data={'text': message.content})
        if self.message_activity.over_limit(limit=self.message_limit):
            print('hit the text message limit')
            await message.channel.send(content='hit the text message limit')
            await message.delete()

    async def on_member_join(self, member: Member):
        print(f'{member.name} joined')
        self.join_activity.add_record(initiator=member.id)
        if self.join_activity.over_limit(limit=self.join_limit):
            print('hit the join limit')

    async def on_member_remove(self, member: Member):
        print(f'{member.name} left')


def start_bot(config):
    bot_token = os.getenv('ANTISPAMBOT_TOKEN')
    if bot_token is None:
        print('missing ANTISPAMBOT_TOKEN environment variable')
        raise NoTokenError()
    bot = AntiSpamBot(config, status=Status.idle)
    bot.http.user_agent = 'AntiSpamBot/0.1'
    bot.run(bot_token, bot=True)


def start_self_bot(config):
    warnings.warn('Running as self-bot, this is only used for testing purposes, proceed at your own risk')
    self_bot_token = os.getenv('SELF_BOT_TOKEN')
    if self_bot_token is None:
        raise NoTokenError()
    bot = AntiSpamBot(config)
    bot.http.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
    bot.run(self_bot_token, bot=False)


class NoTokenError(RuntimeError):
    pass


if __name__ == '__main__':
    with open('config.json', 'rt', encoding='utf8') as f:
        config = json.load(f)
    try:
        start_bot(config)
    except NoTokenError:
        self_bot_enabled = os.getenv('ALLOW_SELF_BOT')
        if self_bot_enabled == 'true':
            start_self_bot(config)
        else:
            exit(1)
