import os
import warnings

from discord import Client, Status, Message, Intents


class AntiSpamBot(Client):
    def __init__(self, *args, **kwargs):
        intents = Intents(messages=True)
        super().__init__(intents=intents, *args, **kwargs)

    async def on_ready(self):
        print(f'We have logged in as {self.user}')

    async def on_message(self, message: Message):
        print('on_message')


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
