import requests


def get_companies() -> list:
    """
    Получение названия компаний и их ID
    """
    companies_data = {
        "ЛУКОЙЛ": 907345,
        "Ozon": 2180,
        "МТС": 3776,
        "СБЕР": 3529,
        "Банк ВТБ": 4181,
        "Газпромнефть": 39305,
        "Альфа-банк": 80,
        "VK": 15478,
        "ПАО Ростелеком": 2748,
        "РЖД": 23427,
    }

    data = []

    for company_name, company_id in companies_data.items():
        company_url = f"https://hh.ru/employer/{company_id}"
        company_info = {"company_id": company_id, "company_name": company_name, "company_url": company_url}
        data.append(company_info)

    return data


def get_vacancies(data: list) -> list[dict]:
    """
    Получение информации о компаниях
    """
    vacancies = []
    for company in data:
        company_id = company["company_id"]
        url = f"https://api.hh.ru/vacancies?employer_id={company_id}&per_page=100"
        response = requests.get(url)
        if response.status_code == 200:
            vacancy = response.json()["items"]
            vacancies.extend(vacancy)
        else:
            print(f"Ошибка при запросе компании {company["company_name"]}: {response.status_code}")
    return vacancies


def get_vacancy_list(data: list) -> list[dict]:
    """
    Преобразование информации о компаниях для дальнейшей работы с БД
    """
    vacancy_list = []
    for item in data:
        company_name = item["employer"]["name"]
        company_id = item["employer"]["id"]
        company_url = item["employer"]["url"]
        job_title = item["name"]
        link_to_vacancy = item["employer"]["alternate_url"]
        salary = item["salary"]
        salary_from = 0
        currency = " "
        description = item["snippet"]["responsibility"]
        experience = item["experience"]["name"]
        requirement = item["snippet"]["requirement"]
        if salary:
            salary_from = item["salary"]["from"]
            if not salary_from:
                salary_from = 0
            currency = item["salary"]["currency"]
            if not currency:
                currency = " "
        if not experience:
            experience = "Информация отсутствует"

        vacancy_list.append(
            {
                "company_id": company_id,
                "company_name": company_name,
                "company_url": company_url,
                "job_title": job_title,
                "link_to_vacancy": link_to_vacancy,
                "salary_from": salary_from,
                "salary_to": salary,
                "currency": currency,
                "experience": experience,
                "description": description,
                "requirement": requirement,
            }
        )
    return vacancy_list
