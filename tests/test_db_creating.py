import pytest
from typing import Any
import psycopg2
from src.db_creating import create_data_base, save_data_to_db
import unittest
from unittest.mock import patch, MagicMock


@pytest.fixture
def test_params() -> Any:
    return {
        "user": "postgres",
        "password": "12345",
        "host": "localhost",
        "port": "5432"
    }


def test_create_data_base(test_params: Any) -> None:
    create_data_base("test_db", test_params)

    with psycopg2.connect(dbname="postgres", **test_params) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database WHERE datname = 'test_db'")
            result = cur.fetchone()
            assert result[0] == 'test_db'


class TestSaveDataToDb(unittest.TestCase):
    def setUp(self):
        self.test_data = [
            {
                "company_id": 1,
                "company_name": "Company 1",
                "company_url": "https://company1.com",
                "job_title": "Software Engineer",
                "link_to_vacancy": "https://company1.com/vacancy/1",
                "salary_from": 50000,
                "currency": "USD",
                "experience": "2+ years",
                "description": "Description 1",
                "requirement": "Requirement 1"
            },
            {
                "company_id": 2,
                "company_name": "Company 2",
                "company_url": "https://company2.com",
                "job_title": "Data Analyst",
                "link_to_vacancy": "https://company2.com/vacancy/1",
                "salary_from": 60000,
                "currency": "USD",
                "experience": "3+ years",
                "description": "Description 2",
                "requirement": "Requirement 2"
            },
            {
                "company_id": 3,
                "company_name": "Company 3",
                "company_url": "https://company3.com",
                "job_title": "Product Manager",
                "link_to_vacancy": "https://company3.com/vacancy/1",
                "salary_from": 70000,
                "currency": "USD",
                "experience": "5+ years",
                "description": "Description 3",
                "requirement": "Requirement 3"
            },
            {
                "company_id": 4,
                "company_name": "Company 4",
                "company_url": "https://company4.com",
                "job_title": "UI/UX Designer",
                "link_to_vacancy": "https://company4.com/vacancy/1",
                "salary_from": 45000,
                "currency": "USD",
                "experience": "1+ years",
                "description": "Description 4",
                "requirement": "Requirement 4"
            }
        ]
        self.test_params = {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "12345"
        }

    @patch('psycopg2.connect')
    def test_save_data_to_db(self, mock_connect):
        mock_cursor = mock_connect.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.execute.return_value = None
        save_data_to_db(self.test_data, "postgres", self.test_params)
        self.assertEqual(mock_cursor.execute.call_count, 0)
