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
        self.message_limit_hit = False
        self.join_activity = RecentActivity(action_name='join')
        self.join_limit = ActivityLimit(**config['JOIN_LIMIT'])
        self.join_limit_hit = False

    async def on_ready(self):
        print(f'We have logged in as {self.user}')

    async def on_message(self, message: Message):
        if message.author.id == self.user.id:
            return
        if not self.enable_message_limit:
            return
        self.message_activity.add_record(initiator=message.author.id, action_data={'message': message})
        if self.message_activity.over_limit(limit=self.message_limit):
            print('hit the text message limit')
            if not self.message_limit_hit:
                self.message_limit_hit = True
                while self.message_activity.activity:
                    message: Message = self.message_activity.activity.pop().data['message']
                    await message.delete()
            else:
                await message.delete()
            await message.channel.send(content='hit the text message limit')
        else:
            self.message_limit_hit = False

    async def on_member_join(self, member: Member):
        print(f'{member.name} joined')
        self.join_activity.add_record(initiator=member.id, action_data={'member': member})
        if self.join_activity.over_limit(limit=self.join_limit):
            print('hit the join limit')
            if not self.join_limit_hit:
                self.join_limit_hit = True
                while self.join_activity.activity:
                    member: Member = self.join_activity.activity.pop().data['member']
                    await member.kick(reason='raid protection')
            else:
                await member.kick(reason='raid protection')
        else:
            self.join_limit_hit = False

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


class NoTokenError(RuntimeError):
    pass


if __name__ == '__main__':
    with open('config.json', 'rt', encoding='utf8') as f:
        config = json.load(f)
    try:
        start_bot(config)
    except NoTokenError:
        exit(1)
