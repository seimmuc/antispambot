import os
import time
import warnings
from typing import List

from discord import Client, Status, Member, Message, Intents, TextChannel


JOIN_LIMIT = {'time_window': 3.0, 'count_limit': 3}
MESSAGE_LIMIT = {'time_window': 5.0, 'count_limit': 1}    # very easy to hit for testing purposes

ENABLE_MESSAGE_LIMIT = True


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
            action_data = self.action_name
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
    def __init__(self, *args, **kwargs):
        intents = Intents(members=True, messages=True)
        super().__init__(intents=intents, *args, **kwargs)
        self.message_activity = RecentActivity(action_name='messages')
        self.message_limit = ActivityLimit(**MESSAGE_LIMIT)
        self.join_activity = RecentActivity(action_name='join')
        self.join_limit = ActivityLimit(**JOIN_LIMIT)

    async def on_ready(self):
        print(f'We have logged in as {self.user}')

    async def on_message(self, message: Message):
        print('on_message')
        if not ENABLE_MESSAGE_LIMIT:
            return
        self.message_activity.add_record(initiator=message.author.id, action_data={'text': message.content})
        if self.message_activity.over_limit(limit=self.message_limit):
            print('hit the text message limit')
            # channel: TextChannel = message.channel
            # await channel.send(content='hit the text message limit')

    async def on_member_join(self, member: Member):
        print(f'{member.name} joined')
        self.join_activity.add_record(initiator=member.id)
        if self.join_activity.over_limit(limit=self.join_limit):
            print('hit the join limit')

    async def on_member_remove(self, member: Member):
        print(f'{member.name} left')


def start_bot():
    bot_token = os.getenv('ANTISPAMBOT_TOKEN')
    if bot_token is None:
        print('missing ANTISPAMBOT_TOKEN environment variable')
        raise NoTokenError()
    bot = AntiSpamBot(status=Status.idle)
    bot.http.user_agent = 'AntiSpamBot/0.1'
    bot.run(bot_token, bot=True)


def start_self_bot():
    warnings.warn('Running as self-bot, this is only used for testing purposes, proceed at your own risk')
    self_bot_token = os.getenv('SELF_BOT_TOKEN')
    if self_bot_token is None:
        raise NoTokenError()
    bot = AntiSpamBot()
    bot.http.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
    bot.run(self_bot_token, bot=False)


class NoTokenError(RuntimeError):
    pass


if __name__ == '__main__':
    try:
        start_bot()
    except NoTokenError:
        self_bot_enabled = os.getenv('ALLOW_SELF_BOT')
        if self_bot_enabled == 'true':
            start_self_bot()
        else:
            exit(1)
