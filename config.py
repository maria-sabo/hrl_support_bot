token = ''
users = []

ssh_host_url = ""
ssh_path_to_key = ""
ssh_username = ""
ssh_port =

postgres_port = 5432


def foreign_db_params(db_name):
    """
    Функция возвращает параметры для подключения к БД.
    :param db_name: Название БД, к которой будет выполняться подключение
    :return: Словарь, содержащий параметры для подключения к БД
    """
    foreign_db_params_ = {
        'dbname': db_name,
        'user': "",
        'password': "",
        'host': "localhost",
        'port':
    }
    return foreign_db_params_


employees_excel_columns = ['ID сотрудника', 'ID физлица', 'ФИО', 'Телефон', 'Email', 'Подтверждён?', 'Кадровик',
                           'Руководитель',
                           'Администратор', 'Юрлицо', 'Дата создания сотрудника', 'Дата изменения сотрудника']
documents_excel_columns = ['ID документа', 'Тип документа', 'Номер документа', 'Дата документа', 'Черновик',
                           'Подписан сотрудником', 'Подписан руководителем', 'Дата последнего изменения']
