from re import L
from telegram import ForceReply, Update
from telegram import ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
import json
import telegram
import logging
import requests
import time
import urllib
import random
import pandas as pd
# import ast
import pickle
import config
from dbhelper import DBHelper
TOKEN = config.token
db = DBHelper()
df = pd.read_pickle('razmetka_telegram_general.pkl')
honeypots = pd.read_csv('honeypots.csv')

def get_honeypot(item=None):
    row = honeypots.sample(n=1).iloc[0]
    item = row['item']
    label = row['class']
    return item, label

def get_item_classes(username):
    tmp = pd.DataFrame(db.get_all_labels(), columns=['user', 'offered', 'item', 'class'])[['item', 'class', 'user']]
    if random.random() < max(1/(2 + len(tmp[tmp['user'] == username])), 0.15):
        item, label = get_honeypot()
        tmpdf = df[df['item'] == item]
        row = tmpdf.sample(n=1).iloc[0]
        item = row['item']
        item = item.replace('>', ' ').replace('<', ' ').replace('&', ' ').replace('*', ' ').replace('#', ' ').replace('_', ' ')
        classes = row['top_4_cats']
        return item, classes
    tmp_grouped = tmp.groupby('item')['class'].count().reset_index()

    unvalidated = pd.DataFrame(db.get_all_unvalidated(), columns=['user', 'item', 'class'])[['item', 'class', 'user']]
    unvalidated_list = list(unvalidated['item'].unique())

    user_already_items = list(tmp[tmp['user'] == username]['item'].unique())
    # filter out items that have 3 labels already
    items_more_than_3 = list(tmp_grouped[tmp_grouped['class'] >= 3]['item'].unique())
    # but do not filter out these ones that have 3 labels but without majority
    mid_list = [item for item in items_more_than_3 if item not in unvalidated_list]
    # filter out items that were labeled before by this user
    final_list = list(set(mid_list + user_already_items))

    curr_df = df[~df['item'].isin(final_list)]
    row = curr_df.sample(n=1).iloc[0]
    item = row['item']
    item = item.replace('>', ' ').replace('<', ' ').replace('&', ' ').replace('*', ' ').replace('#', ' ').replace('_', ' ')
    classes = row['top_4_cats']
    return item, classes

def has_majority(lst):
    return len(lst)-1 >= len(set(lst))

def ban_bad_people():
    tmp = pd.DataFrame(db.get_all_labels(), columns=['user', 'offered', 'item', 'class'])[['item', 'class', 'user']]
    honeypots = pd.read_csv('honeypots.csv')
    tmp = tmp.merge(honeypots, how='inner', on='item')
    tmp = tmp.tail(15)
    tmp = tmp[tmp['class_x'] != tmp['class_y']]
    for user in list(tmp['user']):
        if len(tmp[tmp['user'] == user]) > 3:
            db.add_banned(user)

def go_round_validated():
    labeled = pd.DataFrame(db.get_all_labels(), columns=['user', 'offered', 'item', 'class'])
    three_labeled = labeled.groupby('item')[['user', 'class']].agg(list).reset_index()
    three_labeled = three_labeled[three_labeled['class'].apply(len) == 3]
    validated = three_labeled[three_labeled['class'].apply(has_majority)]
    for idx, row in validated.iterrows():
        usr_list = row['user']
        label_list = row['class']
        item = row['item']
        for user, label in zip(usr_list, label_list):
            db.add_validated(user, item, label)
    unvalidated = three_labeled[~three_labeled['class'].apply(has_majority)]
    for idx, row in unvalidated.iterrows():
        usr_list = row['user']
        label_list = row['class']
        item = row['item']
        for user, label in zip(usr_list, label_list):
            db.add_unvalidated(user, item, label)


# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

from telegram.ext._callbackcontext import CallbackContext
DEFAULT_TYPE = CallbackContext["ExtBot", dict, dict, dict]
# Define a few command handlers. These usually take the two arguments update and
# context.
async def start(update: Update, context: DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    await update.message.reply_html(
        rf"Hi {user.mention_html()}! Send any message (not a command) to start labeling OFD items. We keep a score of how much and how good your labelling is. If you are unsure of category or unaware of item, press the according button. If you need help, write Rollan or Akhmad. To see this message again, press /help",
        reply_markup=ForceReply(selective=True),
    )


async def help_command(update: Update, context: DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    await update.message.reply_text("Send any message (not a command) to start labeling OFD items. We keep a score of how much and how good your labelling is. If you are unsure of category or unaware of item, press the according button. If you need help, write Rollan or Akhmad. To see this message again, press /help")


async def echo(update: Update, context: DEFAULT_TYPE) -> None:
    """Echo the user message."""
    await update.message.reply_text(update.message.text)

def capslock(x : str):
    return x.upper()

def build_keyboard(items):
    keyboard = []
    for i in range(len(items)//2):
        keyboard.append([items[2*i], items[2*i + 1]])
    # keyboard = [['asd', 'bsd']]
    return keyboard



async def botius(update: Update, context: DEFAULT_TYPE) -> None:
    go_round_validated()
    ban_bad_people()

    text = update.message.text
    chat = update.message.chat.id
    username = update.message.from_user.username

    if username in db.get_banned():
        await update.message.reply_text("""You are banned. Please write sumekenov to be unbanned""")

    elif str(chat) in db.get_users_with_registry():
        classes = db.get_offered(chat).split('***')
        item = db.get_item(chat)
        labeled = pd.DataFrame(db.get_all_labels(), columns=['user', 'offered', 'item', 'class'])
        if text in classes:
            db.add_label(username, '***'.join(classes), item, text)
            db.delete_user(chat)
            item, classes = get_item_classes(username)
            print(item, classes)
            keyboard = build_keyboard(classes)
            db.add_user_registry(chat, '***'.join(classes), item)

            # await update.message.reply_text(ReplyKeyboardMarkup)
            # res = "Thank you for your service! Select right answer for \n\n{}".format(item)
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, input_field_placeholder="Choose?")
            await update.message.reply_text("""Thank you for your service! For help, send /help. You labeled {} items. Choose the right answer for \n \n *{}*""".format(len(labeled[labeled['user'] == username]), item),
                reply_markup=reply_markup, parse_mode=telegram.constants.ParseMode('Markdown'))
        else:
            item, classes = get_item_classes(username)
            keyboard = build_keyboard(classes)
            # await update.message.reply_text(ReplyKeyboardMarkup)
            db.add_user_registry(chat, '***'.join(classes), item)
            reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, input_field_placeholder="Choose?")
            await update.message.reply_text("Probably something went wrong:( Please report it to sumekenov. For help, send /help. Choose the right answer for \n \n *{}*".format(item),
                reply_markup=reply_markup, parse_mode=telegram.constants.ParseMode('Markdown'))

    else:
        item, classes = get_item_classes(username)
        print(item, classes)
        keyboard = build_keyboard(classes)
        db.add_user_registry(chat, '***'.join(classes), item)
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, input_field_placeholder="Choose?")
        await update.message.reply_text("Welcome to Aitu label bot! For help, send /help. Choose the right answer \n \n*{}*".format(item),
                reply_markup=reply_markup)
    await update.message.reply_text(ReplyKeyboardMarkup)


def main() -> None:
    """Start the bot."""
    db.setup()
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(TOKEN).build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))

    # on non command i.e message - echo the message on Telegram
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, botius))

    # Run the bot until the user presses Ctrl-C
    application.run_polling()

if __name__ == "__main__":
    main()
