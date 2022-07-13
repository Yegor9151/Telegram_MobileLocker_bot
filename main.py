from locker import Locker_bot
from datetime import datetime
import chat_id


start = datetime.now()

bot = Locker_bot(
    period=('2022-06-01', '2022-06-30'), 
    tg_token='../keys/tg_locker.txt',
    bq_token='../keys/bq_token.json',
    af_token='../keys/af_token.json'
)
bot.read_evetns()
bot.read_frauds()
bot.process()
bot.report()
bot.split_reports()
bot.archive()
bot.telegpush(chat_id=chat_id.TEST1)

print(datetime.now() - start)
