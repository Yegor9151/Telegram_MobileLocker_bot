import chat_ids
import shutil

from utils import last_month
from locker import Locker_bot
from datetime import datetime


CHAT_ID = chat_ids.TEST1
PERIOD_OF_REPORT = tuple(map(str, last_month()))
TG_TOKEN = '../keys/tg_locker.txt'

print(PERIOD_OF_REPORT)

start_time = datetime.now()

bot = Locker_bot(
    chat_id=CHAT_ID,
    period=PERIOD_OF_REPORT,
    tg_token=TG_TOKEN,
    bq_token='../keys/bq_token.json',
    af_token='../keys/af_token.json'
)
bot.create_dirs()
print('directions created\n')

# Collect data
bot.send_message(f'Собираю данные для отчета...\nпериод c {PERIOD_OF_REPORT[0]} до {PERIOD_OF_REPORT[1]}')
bot.read_evetns()
print('events readed\n')
bot.read_frauds()
print('fraud readed\n')

# Assembling report
bot.process()
print('data processed\n')
bot.report()
print('report assembled\n')
bot.split_reports()
print('report splited\n')
bot.archive()
print('report archived\n')

# Pushing report
bot.send_message('Загружаю файлы...')
bot.send_documents('./result')
bot.send_message(f'Готово!\nВремя выполнения: {datetime.now() - start_time}')

print('report pushed\n')
print('time of work:', datetime.now() - start_time)

shutil.rmtree('./data')
shutil.rmtree('./result')
