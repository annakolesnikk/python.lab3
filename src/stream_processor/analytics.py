from collections import Counter
from collections.abc import (
    Callable,
    Iterable,
)

from stream_processor.models import (
    EmployeeRecord,
)


def calculate_statistics(
    employees: Iterable[
        EmployeeRecord
    ],
) -> dict:

    count = 0
    total = 0.0

    highest_paid = None

    departments = Counter()

    for employee in employees:

        count += 1
        total += employee.salary

        departments[
            employee.department
        ] += 1

        if (
            highest_paid is None
            or employee.salary > highest_paid.salary
        ):
            highest_paid = employee

    average = (
        total / count
        if count
        else 0.0
    )

    return {
        "count": count,
        "average_salary": average,
        "total_payroll": total,
        "highest_paid": highest_paid,
        "departments": dict(departments),
    }


def find_first(
    records: Iterable,
    predicate: Callable,
):

    for record in records:

        if predicate(record):
            return record

    return None