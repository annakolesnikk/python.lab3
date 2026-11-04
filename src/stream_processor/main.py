from collections import Counter
from itertools import (
    accumulate,
    count,
    islice,
    pairwise,
)
from pathlib import Path

from stream_processor.analytics import (
    calculate_statistics,
    find_first,
)
from stream_processor.batches import (
    batched_employees,
    export_in_batches,
)
from stream_processor.iterators import (
    EmployeeCollection,
)
from stream_processor.pipeline import (
    build_pipeline,
)


def print_employee(
    employee,
) -> None:

    print(
        f"{employee.employee_id:3} "
        f"{employee.name:20} "
        f"{employee.position:22} "
        f"{employee.department:10} "
        f"{employee.salary:10.2f}"
    )


def demo_iterator() -> None:

    departments = ["Sales", "IT", "Finance"]

    iterator = iter(departments)

    print("Iterable and iterator:")
    print(next(iterator))
    print(next(iterator))
    print(next(iterator))
    print(
        "iter(list) is list:",
        iter(departments) is departments,
    )
    print(
        "iter(iterator) is iterator:",
        iter(iterator) is iterator,
    )
    print(
        "Exhausted iterator:",
        list(iterator),
    )


def main() -> None:

    path = Path(
        "data/input.csv"
    )

    demo_iterator()

    collection = EmployeeCollection(
        list(
            islice(
                build_pipeline(path),
                3,
            )
        )
    )

    print("\nCustom iterator (two passes):")
    print([employee.employee_id for employee in collection])
    print([employee.employee_id for employee in collection])

    ids = islice(
        count(start=1),
        5,
    )

    print(
        "\nInfinite count() + islice:",
        list(ids),
    )

    print("\nFirst 2 employees of IT department:")

    for employee in islice(
        build_pipeline(
            path,
            department="IT",
        ),
        2,
    ):
        print_employee(employee)

    print("\nEmployees with salary >= 2000:")

    for employee in build_pipeline(
        path,
        minimum_salary=2000,
    ):
        print_employee(employee)

    invalid = Counter()

    statistics = calculate_statistics(
        build_pipeline(
            path,
            invalid=invalid,
        )
    )

    print("\nStatistics:")
    print(f"Count: {statistics['count']}")
    print(f"Average salary: {statistics['average_salary']:.2f}")
    print(f"Total payroll: {statistics['total_payroll']:.2f}")
    print(
        "Highest paid:",
        statistics["highest_paid"].name,
        statistics["highest_paid"].salary,
    )
    print("Departments:", statistics["departments"])
    print("Invalid records:", invalid["invalid"])

    salaries = (
        employee.salary
        for employee in build_pipeline(path)
    )

    print(
        "\nPayroll via generator expression:",
        sum(salaries),
    )

    first_salaries = [
        employee.salary
        for employee in islice(
            build_pipeline(path),
            5,
        )
    ]

    print(
        "Running payroll (accumulate):",
        list(accumulate(first_salaries)),
    )

    print("Salary differences (pairwise):")

    for previous, current in pairwise(
        first_salaries
    ):
        print(current - previous)

    print("\nBatches of 3 employees:")

    for batch in batched_employees(
        build_pipeline(path),
        3,
    ):
        print([employee.employee_id for employee in batch])

    batches = export_in_batches(
        build_pipeline(path),
        Path("data/export.csv"),
        batch_size=3,
    )

    print(
        "Batch export to data/export.csv, batches:",
        batches,
    )

    found = find_first(
        build_pipeline(path),
        lambda employee: employee.department == "Finance",
    )

    print(
        "\nFirst Finance employee (early termination):",
        found.name if found else None,
    )


if __name__ == "__main__":
    main()