from db_connect import DbConnection
import psycopg2.extras

psycopg2.extras.register_uuid()


def select_host(host):
    """

    :param host:
    :return:
    """
    db_conn = DbConnection("ekd_metadata")
    curs = db_conn.curs
    db_conn.curs.execute(
        """
        SELECT database_name FROM customer
        JOIN customer_database_connection ON customer.id = customer_database_connection.customer_id
        WHERE customer.host like concat(%s, '.hr-link.ru')
        AND (database_name like '%%ekd_ekd%%' OR database_name like '%%ekd_id%%');
        """, (host.lower(),))
    response = curs.fetchall()
    curs.close()
    return response


def select_users(tenant_db):
    """

    :param tenant_db:
    :return:
    """
    db_conn = DbConnection(tenant_db[0])
    curs = db_conn.curs
    db_conn.curs.execute(
       """
       WITH tmp AS 
            (SELECT 
                    "user".id AS user_id,
                    ul.login,
                    ul.confirmed_date
            FROM "user"
            LEFT JOIN person p ON "user".id = p.user_id
            LEFT JOIN user_login ul ON "user".id = ul.user_id
            WHERE login_type = 'PHONE'),
       tmp2 AS 
            (SELECT 
                    "user".id AS user_id,
                     ul.login,
                     ul.confirmed_date
            FROM "user"
            LEFT JOIN person p ON "user".id = p.user_id
            LEFT JOIN user_login ul ON "user".id = ul.user_id
            WHERE login_type = 'EMAIL')
       SELECT 
            "user".id,
            concat(lASt_name, ' ', first_name, ' ', patronymic) AS name,
            tmp.login                                           AS "Телефон",
            tmp2.login                                          AS "Email",
            CASE
                WHEN (tmp.confirmed_date IS NOT NULL) OR (tmp2.confirmed_date IS NOT NULL)
                    THEN 'Да'
                WHEN (tmp.confirmed_date is null) OR (tmp2.confirmed_date IS NULL)
                    THEN 'Нет'
            END AS "Подтвержден"
       FROM tmp
       FULL OUTER JOIN tmp2 on tmp.user_id = tmp2.user_id
       RIGHT JOIN "user" on (tmp.user_id = "user".id or tmp2.user_id = "user".id)
       FULL OUTER JOIN person p2 on "user".id = p2.user_id;
       """)
    response = curs.fetchall()
    curs.close()
    return response


def select_employees(tenant_db):
    """

    :param tenant_db:
    :return:
    """
    db_conn = DbConnection(tenant_db[0])
    curs = db_conn.curs
    db_conn.curs.execute(
        """
        WITH employee_with_2_roles AS
            (SELECT 
                    employee.id AS employee_id,
                    CASE
                        WHEN count(employee_id) = 2
                            THEN true
                    END AS "tmp_2_roles"
                FROM employee
                LEFT JOIN legal_entity_employee_role leer
                    ON employee.id = leer.employee_id
                GROUP BY employee.id),
        roles AS 
            (SELECT
                    employee_with_2_roles.employee_id AS employee_id,
                    "tmp_2_roles",
                    client_user.user_id,
                    cur.id,
                    le.short_name as "Юрлицо",
                    CASE
                        WHEN (name_key = 'ekd.roles.employee.head.name' AND "tmp_2_roles" IS NULL) OR 
                        ("tmp_2_roles" IS NOT NULL)
                            THEN 'Да'
                        ELSE 'Нет'
                    END AS "Руководитель",
                    CASE
                        WHEN (name_key = 'ekd.roles.employee.hr.name' AND "tmp_2_roles" IS NULL) OR
                        ("tmp_2_roles" IS NOT NULL)
                            THEN 'Да'
                        ELSE 'Нет'
                        END AS "Кадровик",
                    CASE
                        WHEN cur.id IS NOT NULL
                            THEN 'Да'
                        WHEN cur.id IS NULL
                            THEN 'Нет'
                        END AS "Администратор",
                    employee.created_date::text AS "Дата создания сотрудника",
                    employee.modified_date::text AS "Дата изменения сотрудника"
            FROM employee_with_2_roles
            LEFT JOIN legal_entity_employee_role
                ON employee_with_2_roles.employee_id = legal_entity_employee_role.employee_id
            LEFT JOIN employee_role e
                ON e.id = legal_entity_employee_role.employee_role_id
            LEFT JOIN employee
                ON employee_with_2_roles.employee_id = employee.id
            LEFT JOIN legal_entity le
                ON employee.legal_entity_id = le.id
            LEFT JOIN client_user
                ON employee.client_user_id = client_user.id
            LEFT JOIN client_user_role cur
                ON client_user.id = cur.client_user_id
            ORDER BY client_user.user_id),
            final_roles AS 
            (SELECT 
                DISTINCT roles.employee_id,
                roles.user_id,
                '',
                '',
                '',
                '',
                roles."Кадровик",
                roles."Руководитель",
                roles."Администратор",
                roles."Юрлицо",
                roles."Дата создания сотрудника",
                roles."Дата изменения сотрудника"
            FROM roles
            ORDER BY user_id)
        SELECT * FROM final_roles;
        """, )
    response = curs.fetchall()
    curs.close()
    return response


def select_documents(tenant_db):
    """

    :param tenant_db:
    :return:
    """
    db_conn = DbConnection(tenant_db[0])
    curs = db_conn.curs
    db_conn.curs.execute(
        """
        WITH tmp AS 
            (SELECT 
                    DISTINCT document.id AS document_id, 
                    signer_type,
                    signed_date                          ,
                    CASE
                        WHEN ((d.signer_type = 'EMPLOYEE') and (d.signed_date IS NOT NULL))
                            THEN 'Да'
                        ELSE 'Нет'
                    END AS "Подписан сотрудником"
            FROM document 
            LEFT JOIN document_signer d ON document.id = d.document_id
            WHERE signer_type='EMPLOYEE'),
        tmp2 AS 
            (SELECT 
                    document. id AS document_id,
                    CASE
                        WHEN ((d.signer_type = 'HEAD_MANAGER') AND (d.signed_date IS NOT NULL))
                            THEN 'Да'
                        ELSE 'Нет'
                    END AS "Подписан руководителем"
            FROM document LEFT JOIN document_signer d ON document.id = d.document_id
            WHERE signer_type='HEAD_MANAGER'),
        tmp3 AS 
            (SELECT 
                    document.id AS document_id,
                    document_type.russian_name,
                    number,
                    date::text,
                    CASE
                        WHEN document.base_document_id IS NULL
                            THEN 'Да'
                        ELSE 'Нет'
                    END AS "Черновик",
                    document.modified_date::text AS "Дата последнего изменения"
            FROM document
            LEFT JOIN document_type ON document.document_type_id = document_type.id
            WHERE ((document.base_document_id is null) AND (document.sent_date is not null)) IS FALSE)
        SELECT  
                tmp3.document_id, 
                russian_name,
                number,
                date, "Черновик",
                "Подписан сотрудником",
                "Подписан руководителем",
                "Дата последнего изменения"
        FROM tmp3 
        LEFT JOIN tmp ON tmp3.document_id = tmp.document_id 
        LEFT JOIN tmp2 ON tmp3.document_id = tmp2.document_id;
        """, )
    response = curs.fetchall()
    curs.close()
    return response
