import json
import requests
import time
import urllib
import pandas as pd

import config
from dbhelper import DBHelper

db = DBHelper()

TOKEN = config.token
URL = "https://api.telegram.org/bot{}/".format(TOKEN)

df = pd.read_pickle('razmetka_telegram.pkl')
def get_item_classes():
    tmp = pd.DataFrame(db.get_all_labels(), columns=['offered', 'chat', 'item', 'class'])[['item', 'class']]
    tmp = tmp.groupby('item').count().reset_index()
    curr_df = df[~df['item'].isin(list(tmp[tmp['class'] >= 3]['item']))]
    row = df.sample(n=1).iloc[0]
    item = row['item']
    item = item.replace('>', ' ').replace('<', ' ').replace('&', ' ').replace('*', ' ').replace('#', ' ').replace('_', ' ')
    classes = row['top_4_cats']
    if len(classes) != 2:
        classes.append('Все варианты неверны')
    return item, classes
import time
def get_url(url):
    try:
        response = requests.get(url)
        content = response.content.decode("utf8")
    except (ConnectionAbortedError, ConnectionResetError, ConnectionRefusedError, ConnectionError):
        time.sleep(5)
        response = requests.get(url)
        content = response.content.decode("utf8")
    return content


def get_json_from_url(url):
    content = get_url(url)
    js = json.loads(content)
    return js


def get_updates(offset=None):
    url = URL + "getUpdates"
    if offset:
        url += "?offset={}".format(offset)
    js = get_json_from_url(url)
    return js


def get_last_update_id(updates):
    update_ids = []
    for update in updates["result"]:
        update_ids.append(int(update["update_id"]))
    return max(update_ids)



def handle_updates(updates):
    for update in updates["result"]:
        # print(update)
        text = update["message"]["text"]
        chat = update["message"]["chat"]["id"]
        if str(chat) in db.get_users_with_registry():
            classes = db.get_offered(chat).split('***')
            item = db.get_item(chat)
            if text in classes:
                db.add_label(update['message']['from']['username'], '***'.join(classes), item, text)
                db.delete_user(chat)
                item, classes = get_item_classes()
                keyboard = build_keyboard(classes)
                db.add_user_registry(chat, '***'.join(classes), item)
                res = send_message("Thank you for your service! Select right answer for \n\n{}".format(item), chat, keyboard)
            else:
                item, classes = get_item_classes()
                keyboard = build_keyboard(classes)
                res = send_message("Something went wrong. Select right answer for \n\n{}".format(item), chat, keyboard)
                db.add_user_registry(chat, '***'.join(classes), item)
#             keyboard = build_keyboard(items)
#             send_message("Выберите подходящий ответ", chat, keyboard)
#             db.delete_user()
        else:
#             print(chat, db.get_users_with_registry())
            item, classes = get_item_classes()
            keyboard = build_keyboard(classes)
            res = send_message("Select right answer for \n\n{}".format(item), chat, keyboard)
            db.add_user_registry(chat, '***'.join(classes), item)
            


def get_last_chat_id_and_text(updates):
    num_updates = len(updates["result"])
    last_update = num_updates - 1
    text = updates["result"][last_update]["message"]["text"]
    chat_id = updates["result"][last_update]["message"]["chat"]["id"]
    return (text, chat_id)


def build_keyboard(items):
    keyboard = []
    for i in range(len(items)//2):
        keyboard.append([items[2*i], items[2*i + 1]])
    reply_markup = {"keyboard":keyboard, "one_time_keyboard": True}
    return json.dumps(reply_markup)


def send_message(text, chat_id, reply_markup=None):
    text = urllib.parse.quote_plus(text)
    url = URL + "sendMessage?text={}&chat_id={}&parse_mode=Markdown".format(text, chat_id)
    if reply_markup:
        url += "&reply_markup={}".format(reply_markup)
    return get_url(url)


# def main():
db.setup()
s = time.time()
k = 0
last_update_id = None
while True:
    try:
        if time.time() - s > (k * 1200):
            k += 1
            send_message('BOT IS WORKING', 314607865)
        updates = get_updates(last_update_id)
#         print(updates)
        if len(updates["result"]) > 0:
            last_update_id = get_last_update_id(updates) + 1
            handle_updates(updates)
        time.sleep(0.5)
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(e)
        continue
