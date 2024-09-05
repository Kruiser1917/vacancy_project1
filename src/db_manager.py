from typing import Any, Optional
import psycopg2


class DBManager:
    """Класс, который подключается к БД PostgreSQL."""

    def __init__(self, params: Any) -> None:
        """Инициализация класса."""
        self.conn = psycopg2.connect(**params)
        self.cur = self.conn.cursor()

    def create_tables(self) -> None:
        """Создает таблицы companies и vacancies, связанные через внешний ключ."""
        create_companies_table = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            hh_id VARCHAR(50) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            url TEXT
        );
        """
        create_vacancies_table = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            hh_id VARCHAR(50) UNIQUE NOT NULL,
            job_title VARCHAR(255) NOT NULL,
            salary_from INTEGER,
            salary_to INTEGER,
            currency VARCHAR(10),
            link_to_vacancy TEXT,
            company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE
        );
        """
        self.cur.execute(create_companies_table)
        self.cur.execute(create_vacancies_table)
        self.conn.commit()

    def get_companies_and_vacancies_count(self) -> dict[Any, Any]:
        """Получает список всех компаний и количество вакансий у каждой компании."""
        self.cur.execute(
            """
            SELECT companies.name,
            COUNT(vacancies.id) 
            FROM vacancies
            JOIN companies ON vacancies.company_id = companies.id
            GROUP BY companies.name
            """
        )
        rows = self.cur.fetchall()
        return {row[0]: row[1] for row in rows}

    def get_all_vacancies(self) -> Any:
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        self.cur.execute(
            """
            SELECT companies.name, vacancies.job_title, vacancies.salary_from, vacancies.currency, vacancies.link_to_vacancy
            FROM vacancies
            JOIN companies ON vacancies.company_id = companies.id;
            """
        )
        rows = self.cur.fetchall()
        return rows

    def get_avg_salary(self) -> Any:
        """Получает среднюю зарплату по вакансиям."""
        self.cur.execute(
            """
            SELECT AVG(salary_from) FROM vacancies;
            """
        )
        row = self.cur.fetchone()
        return row[0] if row else None

    def get_vacancies_with_higher_salary(self) -> Any:
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        self.cur.execute(
            """
            SELECT job_title, salary_from FROM vacancies
            WHERE salary_from > (SELECT AVG(salary_from) FROM vacancies);
            """
        )
        rows = self.cur.fetchall()
        return rows

    def get_vacancies_with_keyword(self, keyword: str) -> Any:
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова."""
        query = """SELECT * FROM vacancies
                   WHERE LOWER(job_title) LIKE %s;"""
        self.cur.execute(query, (f"%{keyword.lower()}%",))
        return self.cur.fetchall()

    def close(self):
        """Закрывает соединение с БД."""
        self.cur.close()
        self.conn.close()
