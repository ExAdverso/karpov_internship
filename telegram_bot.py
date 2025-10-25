import telebot
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd

my_token = '..'
bot = telebot.TeleBot(token=my_token)
chat_id = '286733275'

msg = 'Hello'
bot.send_message(chat_id=chat_id, text=msg)