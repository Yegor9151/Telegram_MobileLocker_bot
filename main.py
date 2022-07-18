from utils import last_month
from locker import Locker_bot
from datetime import datetime
import chat_id


start = datetime.now()
period = tuple(map(str, last_month()))
print(period)

bot = Locker_bot(
    period=period,
    tg_token='../keys/tg_locker.txt',
    bq_token='../keys/bq_token.json',
    af_token='../keys/af_token.json'
)
bot.create_dirs()
print('directions created\n')
bot.read_evetns()
print('events readed\n')
bot.read_frauds()
print('fraud readed\n')
bot.process()
print('data processed\n')
bot.report()
print('report assembled\n')
bot.split_reports()
print('report splited\n')
bot.archive()
print('report archived\n')
bot.telegpush(chat_id=chat_id.TEST1)
print('report pushed\n')

print('time of work:', datetime.now() - start)
