import os

import sshtunnel

from telebot import types

import config
import telebot

from db_requests import select_host
from info2excel import select_documents2excel, select_employees2excel

bot = telebot.TeleBot(config.token)

initial_keyboard = telebot.types.ReplyKeyboardMarkup()
initial_keyboard.row('Выбор тенанта')

main_keyboard = telebot.types.ReplyKeyboardMarkup()
main_keyboard.row('Отчёт по пользователям', 'Отчёт по документам', 'Выход')


@bot.message_handler(commands=['start'], func=lambda message: message.chat.id in config.users)
def start_message(message):
    bot.send_message(message.chat.id, 'Привет! Я бот HR-Link! Добро пожаловать!', reply_markup=initial_keyboard)


@bot.message_handler(content_types=['text'], func=lambda message: message.chat.id in config.users)
def send_text(message):
    if message.text.lower() == 'выбор тенанта':
        bot.send_message(message.chat.id, 'Введите домен тенанта (без .hr-link.ru):',
                         reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(message, check_tenant)
    if message.text.lower() == 'выход':
        start_message(message)


@bot.message_handler(func=lambda message: message.chat.id in config.users)
def check_tenant(message: types.Message):
    try:
        host = message.text
        tenant_databases = select_host(host)
        if tenant_databases:
            bot.send_message(message.chat.id, 'Корректный домен. Продолжим работу!', reply_markup=main_keyboard)
            bot.register_next_step_handler(message, action, tenant_databases)
        else:
            bot.send_message(message.chat.id, 'Такого домена нет. Введите еще раз.')
            bot.register_next_step_handler(message, check_tenant)
    except Exception:
        bot.send_message(message.chat.id, 'Что-то пошло не так... Введите еще раз.')
        bot.register_next_step_handler(message, check_tenant)


@bot.message_handler(func=lambda message: message.chat.id in config.users)
def action(message, tenant_databases):
    try:
        if message.text.lower() == 'отчёт по пользователям':
            path2file = r'C:\Users\Maria\PycharmProjects\hrl_support_bot\employee_report_' + str(
                message.chat.id) + '.xlsx'
            select_employees2excel(tenant_databases, path2file)
            f = open(path2file, 'rb')
            bot.send_document(message.chat.id, f)
            f.close()
            os.remove(path2file)
            bot.register_next_step_handler(message, action, tenant_databases)
        elif message.text.lower() == 'отчёт по документам':
            path2file = r'C:\Users\Maria\PycharmProjects\hrl_support_bot\documents_report_' + str(
                message.chat.id) + '.xlsx'
            select_documents2excel(tenant_databases, path2file)
            f = open(path2file, 'rb')
            bot.send_document(message.chat.id, f)
            f.close()
            os.remove(path2file)
            bot.register_next_step_handler(message, action, tenant_databases)
        elif message.text.lower() == 'выход':
            start_message(message)
        else:
            bot.send_message(message.chat.id, 'Что-то пошло не так...')
            bot.register_next_step_handler(message, action, tenant_databases)
    except Exception:
        bot.send_message(message.chat.id, 'Что-то пошло не так...')
        bot.register_next_step_handler(message, action, tenant_databases)


if __name__ == '__main__':
    with sshtunnel.SSHTunnelForwarder(
            (config.ssh_host_url, config.ssh_port),
            ssh_private_key=config.ssh_path_to_key,
            ssh_username=config.ssh_username,
            remote_bind_address=('localhost', config.postgres_port),
            local_bind_address=('localhost', config.ssh_port)) as server:
        server.start()
        while True:
            try:
                bot.infinity_polling()
            except Exception:
                pass
