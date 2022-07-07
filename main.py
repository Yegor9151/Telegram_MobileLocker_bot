from locker import Locker_bot
from datetime import datetime

start = datetime.now()

bot = Locker_bot(period=('2022-06-01', '2022-06-30'), token='../keys/locker.txt')
bot.read_dataframes()
bot.process()
bot.report()
bot.archive()
bot.telegpush(chat_id=123456789)

print(datetime.now() - start)
