import config
from db_requests import select_documents, select_employees, select_users
from user import User

import pandas as pd


def select_documents2excel(tenant_db, path2file):
    df = pd.DataFrame(select_documents(tenant_db[0]), columns=config.documents_excel_columns)
    df.to_excel(path2file)


def select_employees2excel(tenant_db, path2file):
    df = pd.DataFrame(select_employees(tenant_db[0]), columns=config.employees_excel_columns)

    user_data = select_users(tenant_db[1])
    user_ids = [item[0] for item in user_data]
    user_names = [item[1] for item in user_data]
    user_phones = [item[2] for item in user_data]
    user_emails = [item[3] for item in user_data]
    user_confirmed = [item[4] for item in user_data]

    user_list = []
    for i in range(len(df['ID физлица'])):
        tmp_user = User()
        tmp_user.user_id = df['ID физлица'][i]
        user_list.insert(i, tmp_user)

    for k in range(len(user_list)):
        for j in range(len(user_ids)):
            if user_list[k].user_id == user_ids[j]:
                user_list[k].user_name = user_names[j]
                user_list[k].email = user_emails[j]
                user_list[k].phone = user_phones[j]
                user_list[k].confirmed = user_confirmed[j]
    user_names2excel = []
    user_emails2excel = []
    user_phones2excel = []
    user_confirmed2excel = []

    for d in range(len(user_list)):
        user_names2excel.insert(d, user_list[d].user_name)
        user_emails2excel.insert(d, user_list[d].email)
        user_phones2excel.insert(d, user_list[d].phone)
        user_confirmed2excel.insert(d, user_list[d].confirmed)

    df['ФИО'] = user_names2excel
    df['Email'] = user_emails2excel
    df['Телефон'] = user_phones2excel
    df['Подтверждён?'] = user_confirmed2excel

    df.sort_values(['ФИО'], inplace=True)
    df.to_excel(path2file)
