from typing import Any

import psycopg2


def create_data_base(database_name: str, params: Any) -> None:
    """
    Создание базы данных и таблиц с данными о компаниях и вакансиях
    """

    conn = psycopg2.connect(**params)
    conn.autocommit = True

    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    cur.close()
    conn.close()

    with psycopg2.connect(dbname=database_name, **params) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS companies (
            company_id int,
            company_name VARCHAR(255),
            company_url TEXT
            )
            """
            )

        with conn.cursor() as cur:
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS vacancies (
            company_name VARCHAR(255),
            job_title VARCHAR,
            link_to_vacancy TEXT,
            salary_from int,
            currency VARCHAR(20),
            experience TEXT,
            description TEXT,
            requirement TEXT
            )
            """
            )
    conn.commit()
    conn.close()


def save_data_to_db(data: list[dict[Any, Any]], database_name: str, params: Any) -> None:
    """
    Заполнение таблиц данными
    """
    insert_q = """
        INSERT INTO vacancies (company_name, job_title, link_to_vacancy, salary_from, currency, experience,
        description, requirement)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    insert_q_2 = """
                 INSERT INTO companies (company_id, company_name, company_url) VALUES (%s, %s, %s)
                 """
    with psycopg2.connect(dbname=database_name, **params) as conn:
        with conn.cursor() as cur:
            for vacancy in data:
                cur.execute(insert_q_2, (vacancy["company_id"], vacancy["company_name"], vacancy["company_url"]))
                cur.execute(
                    insert_q,
                    (
                        vacancy["company_name"],
                        vacancy["job_title"],
                        vacancy["link_to_vacancy"],
                        vacancy["salary_from"],
                        vacancy["currency"],
                        vacancy["experience"],
                        vacancy["description"],
                        vacancy["requirement"],
                    ),
                )
    conn.commit()
    conn.close()
