from db_connect import DbConnection
import psycopg2.extras

psycopg2.extras.register_uuid()


def select_host(host):
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
    db_conn = DbConnection(tenant_db[0])
    curs = db_conn.curs
    db_conn.curs.execute(
        """
        with tmp as (select "user".id as user_id,
                    ul.login,
                    ul.confirmed_date
             from "user"
                      left join person p on "user".id = p.user_id
                      left join user_login ul on "user".id = ul.user_id
             where login_type = 'PHONE')
        ,
     tmp2 as (select "user".id as user_id,
                     ul.login,
                     ul.confirmed_date
              from "user"
                       left join person p on "user".id = p.user_id
                       left join user_login ul on "user".id = ul.user_id
              where login_type = 'EMAIL')
select "user".id,
       concat(last_name, ' ', first_name, ' ', patronymic) as name,
       tmp.login                                           as "Телефон",
       tmp2.login                                          as "Email",
       case
           when (tmp.confirmed_date is not null) or (tmp2.confirmed_date is not null)
               then 'Да'
           when (tmp.confirmed_date is null) or (tmp2.confirmed_date is null)
               then 'Нет'
           end                                             as "Подтвержден"
from tmp
         full outer join tmp2 on tmp.user_id = tmp2.user_id
         right join "user" on (tmp.user_id = "user".id or tmp2.user_id = "user".id)
         full outer join person p2 on "user".id = p2.user_id
        """)
    response = curs.fetchall()
    curs.close()
    return response


def select_employees(tenant_db):
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
roles AS (
    SELECT
           employee_with_2_roles.employee_id AS employee_id,
           "tmp_2_roles",
           client_user.user_id,
           cur.id,
           le.short_name as "Юрлицо",
           CASE
               WHEN (name_key = 'ekd.roles.employee.head.name' AND "tmp_2_roles" IS NULL) OR
                    "tmp_2_roles" IS NOT NULL
                   THEN 'Да'
               ELSE 'Нет'
           END AS "Руководитель",
           CASE
               WHEN (name_key = 'ekd.roles.employee.hr.name' AND "tmp_2_roles" IS NULL) OR
                     "tmp_2_roles" IS NOT NULL
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
final_roles AS (
SELECT DISTINCT roles.employee_id,
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
    db_conn = DbConnection(tenant_db[0])
    curs = db_conn.curs
    db_conn.curs.execute(
        """
            with tmp as (select distinct document.id as document_id, signer_type, signed_date                          ,
                    case
           when ((d.signer_type = 'EMPLOYEE') and (d.signed_date is not null))
                            then 'Да'
                            else 'Нет'
                        end as "Подписан сотрудником"
                        from
    document left join document_signer d on document.id = d.document_id
     where signer_type='EMPLOYEE'),
tmp2 as (select document. id as document_id, case
           when ((d.signer_type = 'HEAD_MANAGER') and (d.signed_date is not null))
                            then 'Да'
                            else 'Нет'
                        end as "Подписан руководителем"
from document left join document_signer d on document.id = d.document_id
where signer_type='HEAD_MANAGER'),
     tmp3 as (select document.id as document_id,
       document_type.russian_name,
       number, date::text , case
                          when document.base_document_id is null
                            then 'Да'
                            else 'Нет'
                        end as "Черновик", document.modified_date::text as "Дата последнего изменения"
from document
left join document_type on document.document_type_id = document_type.id
where ((document.base_document_id is null) and  (document.sent_date is not null)) is false)
select  tmp3.document_id, russian_name, number, date, "Черновик", "Подписан сотрудником", "Подписан руководителем", "Дата последнего изменения"
                from tmp3 left join tmp on tmp3.document_id = tmp.document_id left join tmp2 on tmp3.document_id = tmp2.document_id


            """, )
    response = curs.fetchall()
    curs.close()
    return response
