import psycopg2
from datetime import datetime

def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS department_employee(
            department VARCHAR(100),
            department_division   VARCHAR(50),
            position_title VARCHAR(50),
            hire_date DATE,
            salary decimal
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.department_employee_salary (
            department varchar(100) NOT NULL,
            total_salary int4 NULL,
            CONSTRAINT department_employee_salary_pk PRIMARY KEY (department)
        );
        """
    )
    conn = None
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        # Create tables
        for command in commands:
            cur.execute(command)
        # Commit the changes
        conn.commit()
        # Close communication with the database
        cur.close()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    create_tables()