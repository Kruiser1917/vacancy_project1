import pytest
import requests
from unittest.mock import patch, MagicMock

from src.api_hh import get_companies, get_vacancies, get_vacancy_list


@pytest.fixture
def company_data():
    return [
        {"company_id": 907345, "company_name": "ЛУКОЙЛ", "company_url": "https://api.hh.ru/employers/907345"},
        {"company_id": 2180, "company_name": "Ozon", "company_url": "https://api.hh.ru/employers/2180"},
        {"company_id": 3776, "company_name": "МТС", "company_url": "https://api.hh.ru/employers/3776"},
        {"company_id": 3529, "company_name": "СБЕР", "company_url": "https://hh.ru/employer/3529"},
        {"company_id": 4181, "company_name": "Банк ВТБ", "company_url": "https://hh.ru/employer/4181"},
        {"company_id": 39305, "company_name": "Газпромнефть", "company_url": "https://hh.ru/employer/39305"},
        {"company_id": 80, "company_name": "Альфа-банк", "company_url": "https://hh.ru/employer/80"},
        {"company_id": 15478, "company_name": "VK", "company_url": "https://hh.ru/employer/15478"},
        {"company_id": 2748, "company_name": "ПАО Ростелеком", "company_url": "https://hh.ru/employer/2748"},
        {"company_id": 23427, "company_name": "РЖД", "company_url": "https://hh.ru/employer/23427"}

    ]


def test_get_companies() -> None:
    company_data = [
        {'company_id': 907345, 'company_name': 'ЛУКОЙЛ', 'company_url': 'https://hh.ru/employer/907345'},
        {'company_id': 2180, 'company_name': 'Ozon', 'company_url': 'https://hh.ru/employer/2180'},
        {'company_id': 3776, 'company_name': 'МТС', 'company_url': 'https://hh.ru/employer/3776'},
        {"company_id": 3529, "company_name": "СБЕР", "company_url": "https://hh.ru/employer/3529"},
        {"company_id": 4181, "company_name": "Банк ВТБ", "company_url": "https://hh.ru/employer/4181"},
        {"company_id": 39305, "company_name": "Газпромнефть", "company_url": "https://hh.ru/employer/39305"},
        {"company_id": 80, "company_name": "Альфа-банк", "company_url": "https://hh.ru/employer/80"},
        {"company_id": 15478, "company_name": "VK", "company_url": "https://hh.ru/employer/15478"},
        {"company_id": 2748, "company_name": "ПАО Ростелеком", "company_url": "https://hh.ru/employer/2748"},
        {"company_id": 23427, "company_name": "РЖД", "company_url": "https://hh.ru/employer/23427"}

    ]
    companies = get_companies()
    assert companies == company_data


@patch('requests.get')
def test_get_vacancies(mock_get, company_data):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'items': [
            {'name': 'Вакансия 1', 'employer': {'id': 907345, 'name': 'ЛУКОЙЛ',
                                                'url': 'https://api.hh.ru/employers/907345'}},
            {'name': 'Вакансия 2', 'employer': {'id': 2180, 'name': 'Ozon', 'url': 'https://api.hh.ru/employers/2180'}},
            {'name': 'Вакансия 3', 'employer': {'id': 3776, 'name': 'МТС', 'url': 'https://api.hh.ru/employers/3776'}}
        ]
    }
    mock_get.return_value = mock_response

    vacancies = get_vacancies(company_data)

    assert len(vacancies) == 30
    assert vacancies[0]['name'] == 'Вакансия 1'
    assert vacancies[0]['employer']['id'] == 907345
    assert vacancies[1]['name'] == 'Вакансия 2'
    assert vacancies[1]['employer']['id'] == 2180
    assert vacancies[2]['name'] == 'Вакансия 3'
    assert vacancies[2]['employer']['id'] == 3776


def test_get_vacancy_list() -> None:
    vacancies = [
        {
            'name': 'Вакансия 1',
            'employer': {'id': 907345, 'name': 'ЛУКОЙЛ', 'url': 'https://api.hh.ru/employers/907345',
                         'alternate_url': 'https://hh.ru/vacancy/1'},
            'salary': {'from': 100000, 'currency': 'RUR'},
            'snippet': {'responsibility': 'Описание вакансии 1', 'requirement': 'Требования к вакансии 1'},
            'experience': {'name': 'Опыт работы 1-3 года'}
        },
        {
            'name': 'Вакансия 2',
            'employer': {'id': 2180, 'name': 'Ozon', 'url': 'https://api.hh.ru/employers/2180',
                         'alternate_url': 'https://hh.ru/vacancy/2'},
            'salary': {'from': 80000, 'currency': 'RUR'},
            'snippet': {'responsibility': 'Описание вакансии 2', 'requirement': 'Требования к вакансии 2'},
            'experience': {'name': 'Без опыта'}
        },
        {
            'name': 'Вакансия 3',
            'employer': {'id': 3776, 'name': 'МТС', 'url': 'https://api.hh.ru/employers/3776',
                         'alternate_url': 'https://hh.ru/vacancy/3'},
            'salary': {'from': 120000, 'currency': 'RUR'},
            'snippet': {'responsibility': 'Описание вакансии 3', 'requirement': 'Требования к вакансии 3'},
            'experience': {'name': 'Более 6 лет'}
        }
    ]
    vacancy_list = get_vacancy_list(vacancies)
    assert len(vacancy_list) == 3
