import pytest
from app.department_analyzer.department_analyzer import DepartmentAnalyzer


@pytest.fixture(scope="module")
def department_analyzer():
    return DepartmentAnalyzer("test_app")


def test_read_data(department_analyzer):
    # Arrange
    departments_path = "data/departments.csv"
    employees_path = "data/employees.csv"

    # Act
    department_analyzer.read_data(departments_path, employees_path)

    # Assert
    assert department_analyzer.departments_df is not None
    assert department_analyzer.employees_df is not None


def test_show_departments(department_analyzer, capsys):
    # Act
    department_analyzer.show_departments()

    # Assert
    captured = capsys.readouterr()
    assert "Sales" in captured.out


def test_show_employees(department_analyzer, capsys):
    # Act
    department_analyzer.show_employees()

    # Assert
    captured = capsys.readouterr()
    assert "salary" in captured.out


def test_calculate_percentage_no_manager(department_analyzer):
    # Act
    result_df = department_analyzer.calculate_percentage_no_manager()

    # Assert
    assert result_df is not None
    assert "%_departments_no_manager" in result_df.columns
    assert result_df.count() == 1
    assert result_df.head()["%_departments_no_manager"] == pytest.approx(25.0, abs=0.01)
