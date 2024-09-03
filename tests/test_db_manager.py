from unittest.mock import patch
from src.db_manager import DBManager

# Пример данных для тестирования
mock_data = {
    "get_companies_and_vacancies_count": [
        ("Company A", 5),
        ("Company B", 3),
        ("Company C", 2),
    ],
    "get_all_vacancies": [
        ("Company A", "Job 1", 50000, "USD", "https://example.com/job1"),
        ("Company B", "Job 2", 60000, "USD", "https://example.com/job2"),
        ("Company C", "Job 3", 70000, "USD", "https://example.com/job3"),
    ],
    "get_avg_salary": [(55000.0,)],
    "get_vacancies_with_higher_salary": [
        ("Job 2", 60000),
        ("Job 3", 70000),
    ],
    "get_vacancies_with_keyword": [
        ("Company A", "Job 1", 50000, "USD", "https://example.com/job1"),
        ("Company B", "Job 2", 60000, "USD", "https://example.com/job2"),
    ],
}

@patch("psycopg2.connect")
def test_db_manager(mock_connect):
    # Создаем экземпляр класса DBManager
    db_manager = DBManager(params={})

    # Тестируем метод get_companies_and_vacancies_count
    mock_cursor = mock_connect.return_value.cursor.return_value
    mock_cursor.fetchall.return_value = mock_data["get_companies_and_vacancies_count"]
    companies_and_vacancies = db_manager.get_companies_and_vacancies_count()
    assert companies_and_vacancies == {
        "Company A": 5,
        "Company B": 3,
        "Company C": 2,
    }

    # Тестируем метод get_all_vacancies
    mock_cursor.fetchall.return_value = mock_data["get_all_vacancies"]
    all_vacancies = db_manager.get_all_vacancies()
    assert all_vacancies == mock_data["get_all_vacancies"]

    # Тестируем метод get_avg_salary
    mock_cursor.fetchall.return_value = mock_data["get_avg_salary"]
    avg_salary = db_manager.get_avg_salary()
    assert avg_salary == mock_data["get_avg_salary"]

    # Тестируем метод get_vacancies_with_higher_salary
    mock_cursor.fetchall.return_value = mock_data["get_vacancies_with_higher_salary"]
    higher_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
    assert higher_salary_vacancies == mock_data["get_vacancies_with_higher_salary"]

    # Тестируем метод get_vacancies_with_keyword
    mock_cursor.fetchall.return_value = mock_data["get_vacancies_with_keyword"]
    vacancies_with_keyword = db_manager.get_vacancies_with_keyword("Job")
    assert vacancies_with_keyword == mock_data["get_vacancies_with_keyword"]