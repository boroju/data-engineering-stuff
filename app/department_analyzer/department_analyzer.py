from pyspark.sql import SparkSession
import pyspark.sql.functions as F


class SparkSessionHandler:
    def __init__(self, app_name="myapp"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()


class DepartmentAnalyzer(SparkSessionHandler):
    def __init__(self, app_name="myapp"):
        super().__init__(app_name)

    def read_data(self, departments_path, employees_path):
        self.departments_df = self.spark.read.format("csv").option("header", "true").load(departments_path)
        self.employees_df = self.spark.read.format("csv").option("header", "true").load(employees_path)

    def show_departments(self):
        return self.departments_df.show(5)

    def show_employees(self):
        return self.employees_df.show(5)

    def calculate_percentage_no_manager(self):
        percentage_no_manager = self.departments_df.select(
            F.round((F.sum(F.when(F.col("mgr_id").isNull(), 1).otherwise(0)) / F.count("*")) * 100, 2)
            .alias("%_departments_no_manager")
        )
        return percentage_no_manager


if __name__ == "__main__":
    # Initialize DepartmentAnalyzer
    department_analyzer = DepartmentAnalyzer("myapp")

    # Read data
    department_analyzer.read_data("data/departments.csv", "data/employees.csv")

    # Show departments
    print(department_analyzer.show_departments())

    # Show employees
    print(department_analyzer.show_employees())

    # Calculate and show percentage of departments without a manager
    print(department_analyzer.calculate_percentage_no_manager().show())