from collections import Counter
from collections.abc import (
    Iterable,
    Iterator,
)

from stream_processor.models import (
    EmployeeRecord,
)


def validate_employees(
    rows: Iterable[
        dict[str, str]
    ],
    invalid: Counter | None = None,
) -> Iterator[EmployeeRecord]:

    for row in rows:

        try:

            employee_id = int(
                row["employee_id"]
            )

            name = row[
                "name"
            ].strip()

            position = row[
                "position"
            ].strip()

            department = row[
                "department"
            ].strip()

            salary = float(
                row["salary"]
            )

        except (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
        ):
            if invalid is not None:
                invalid["invalid"] += 1
            continue

        if (
            not name
            or not position
            or not department
            or salary < 0
        ):
            if invalid is not None:
                invalid["invalid"] += 1
            continue

        yield EmployeeRecord(
            employee_id=employee_id,
            name=name,
            position=position,
            department=department,
            salary=salary,
        )


def filter_by_department(
    employees: Iterable[
        EmployeeRecord
    ],
    department: str | None,
) -> Iterator[EmployeeRecord]:

    for employee in employees:

        if (
            department is None
            or employee.department.lower()
            == department.lower()
        ):
            yield employee


def filter_by_salary(
    employees: Iterable[
        EmployeeRecord
    ],
    minimum: float,
) -> Iterator[EmployeeRecord]:

    for employee in employees:

        if (
            employee.salary
            >= minimum
        ):
            yield employee