import psycopg2
from src.api_hh import get_companies, get_vacancies, get_vacancy_list
from src.config import config
from src.db_creating import create_data_base, save_data_to_db
from src.db_manager import DBManager

params = config()
data_company = get_companies()
data = get_vacancies(data_company)
vacancies = get_vacancy_list(data)

user_choise = input("Введите имя базы данных в которую вы хотите сохранить данные полученные с HH.ru: ")

create_data_base(user_choise, params)
conn = psycopg2.connect(dbname=user_choise, **params)
save_data_to_db(vacancies, user_choise, params)


def main() -> None:
    """
    Функция для взаимодействия с пользователем
    """
    while True:
        print("Программа выполнила запрос:")
        db_manager = DBManager(params)
        print(
            "Выберите запрос: \n"
            "1 - Список всех компаний и количество вакансий у каждой компании\n"
            "2 - Список всех вакансий с указанием названия компании, "
            "названия вакансии и зарплаты и ссылки на вакансию\n"
            "3 - Средняя зарплата по вакансиям\n"
            "4 - Список всех вакансий, у которых зарплата выше средней по всем вакансиям\n"
            "5 - Список всех вакансий, в названии которых содержатся запрашиваемое слово\n"
        )
        user_answer = input("Введите номер запроса\n")
        if user_answer == "1":
            companies_and_vacancies_count = db_manager.get_companies_and_vacancies_count()
            print(f"Список всех компаний и количество вакансий у каждой компании: {companies_and_vacancies_count}\n")
            choice = input("Завершить программу? Выберите да или нет: ")
            if choice == "да":
                print("Программа завершена")
                break
            else:
                continue

        elif user_answer == "2":
            all_vacancies = db_manager.get_all_vacancies()
            print(
                f"Список всех вакансий с указанием названия компании, "
                f"названия вакансии и зарплаты и ссылки на вакансию: {all_vacancies}\n"
            )
            choice = input("Завершить программу? Выберите да или нет: ")
            if choice == "да":
                print("Программа завершена")
                break
            else:
                continue

        elif user_answer == "3":
            avg_salary = db_manager.get_avg_salary()
            print(f"Средняя зарплата по вакансиям: {avg_salary}\n")
            choice = input("Завершить программу? Выберите да или нет: ")
            if choice == "да":
                print("Программа завершена")
                break
            else:
                continue

        elif user_answer == "4":
            vacancies_with_higher_salary = db_manager.get_vacancies_with_higher_salary()
            print(
                f"Список всех вакансий, у которых зарплата выше средней по всем "
                f"вакансиям: {vacancies_with_higher_salary}\n"
            )
            choice = input("Завершить программу? Выберите да или нет: ")
            if choice == "да":
                print("Программа завершена")
                break
            else:
                continue
        elif user_answer == "5":
            user_input = input("Введите слово: ")
            vacancies_with_keyword = db_manager.get_vacancies_with_keyword(user_input)
            print(f"Список всех вакансий, в названии которых содержатся запрашиваемое слово: {vacancies_with_keyword}")
            choice = input("Завершить программу? Выберите да или нет: ")
            if choice == "да":
                print("Программа завершена")
                break
            else:
                continue
        else:
            print("Введен неверный запрос")
            continue


if __name__ == "__main__":
    main()
