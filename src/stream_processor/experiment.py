import csv
import random
import tracemalloc
from pathlib import Path
from time import perf_counter

from stream_processor.analytics import (
    calculate_statistics,
    find_first,
)
from stream_processor.pipeline import (
    build_pipeline,
)


def generate_test_file(
    path: Path,
    count: int,
) -> None:

    departments = (
        "Sales",
        "IT",
        "Finance",
        "HR",
        "Marketing",
    )

    positions = (
        "Manager",
        "Developer",
        "Accountant",
        "Designer",
        "Analyst",
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:

        writer = csv.writer(
            file
        )

        writer.writerow(
            [
                "employee_id",
                "name",
                "position",
                "department",
                "salary",
            ]
        )

        for index in range(
            1,
            count + 1,
        ):

            writer.writerow(
                [
                    index,
                    f"Employee {index}",
                    random.choice(
                        positions
                    ),
                    random.choice(
                        departments
                    ),
                    round(
                        random.uniform(
                            800,
                            5000,
                        ),
                        2,
                    ),
                ]
            )


def read_all_employees(
    path: Path,
) -> list[dict]:

    with path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:

        reader = csv.DictReader(
            file
        )

        return list(reader)


def read_employees_lazy(
    path: Path,
):

    with path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as file:

        reader = csv.DictReader(
            file
        )

        yield from reader


def consume_eager(
    path: Path,
) -> int:

    count = 0

    for _ in read_all_employees(
        path
    ):
        count += 1

    return count


def consume_lazy(
    path: Path,
) -> int:

    count = 0

    for _ in read_employees_lazy(
        path
    ):
        count += 1

    return count


def measure_time(
    function,
    *args,
):

    start = perf_counter()

    result = function(
        *args
    )

    elapsed = perf_counter() - start

    return result, elapsed


def measure_peak_memory(
    function,
    *args,
):

    tracemalloc.start()

    result = function(
        *args
    )

    current, peak = (
        tracemalloc.get_traced_memory()
    )

    tracemalloc.stop()

    return result, peak


def eager_first_result(
    path: Path,
) -> dict:

    return read_all_employees(
        path
    )[0]


def lazy_first_result(
    path: Path,
) -> dict:

    return next(
        read_employees_lazy(
            path
        )
    )


def is_target(
    row: dict,
) -> bool:

    return (
        row["department"] == "IT"
        and float(row["salary"]) > 4990
    )


def eager_find_first(
    path: Path,
) -> dict | None:

    filtered = [
        row
        for row in read_all_employees(
            path
        )
        if is_target(row)
    ]

    return filtered[0] if filtered else None


def lazy_find_first(
    path: Path,
) -> dict | None:

    return find_first(
        read_employees_lazy(
            path
        ),
        is_target,
    )


def run_experiment(
    sizes: tuple[int, ...] = (
        10_000,
        100_000,
        500_000,
    ),
) -> None:

    print(
        f"{'Records':>8} {'Eager time':>11} {'Lazy time':>10} "
        f"{'Eager peak':>11} {'Lazy peak':>10} "
        f"{'Eager first':>12} {'Lazy first':>11}"
    )
    print("-" * 80)

    for size in sizes:

        path = Path(
            f"data/employees_{size}.csv"
        )

        generate_test_file(
            path,
            size,
        )

        _, eager_time = measure_time(
            consume_eager,
            path,
        )
        _, lazy_time = measure_time(
            consume_lazy,
            path,
        )
        _, eager_peak = measure_peak_memory(
            consume_eager,
            path,
        )
        _, lazy_peak = measure_peak_memory(
            consume_lazy,
            path,
        )
        _, eager_first = measure_time(
            eager_first_result,
            path,
        )
        _, lazy_first = measure_time(
            lazy_first_result,
            path,
        )

        print(
            f"{size:>8} {eager_time:>10.3f}s {lazy_time:>9.3f}s "
            f"{eager_peak / 1024 / 1024:>9.2f}MB "
            f"{lazy_peak / 1024 / 1024:>8.2f}MB "
            f"{eager_first:>11.5f}s {lazy_first:>10.5f}s"
        )


def run_early_termination(
    size: int = 1_000_000,
) -> None:

    path = Path(
        f"data/employees_{size}.csv"
    )

    generate_test_file(
        path,
        size,
    )

    eager_record, eager_time = measure_time(
        eager_find_first,
        path,
    )
    lazy_record, lazy_time = measure_time(
        lazy_find_first,
        path,
    )

    print(
        f"\nEarly termination ({size} records):"
    )
    print(
        "Eager result:",
        eager_record["employee_id"] if eager_record else None,
        f"time: {eager_time:.3f}s",
    )
    print(
        "Lazy result:",
        lazy_record["employee_id"] if lazy_record else None,
        f"time: {lazy_time:.5f}s",
    )


def run_large_pipeline(
    size: int = 100_000,
) -> None:

    path = Path(
        f"data/employees_{size}.csv"
    )

    statistics = calculate_statistics(
        build_pipeline(
            path,
        )
    )

    print(
        f"\nPipeline on {size} records:"
    )
    print(f"Count: {statistics['count']}")
    print(f"Average salary: {statistics['average_salary']:.2f}")
    print(f"Total payroll: {statistics['total_payroll']:.2f}")
    print("Departments:", statistics["departments"])


def main() -> None:

    run_experiment()

    run_large_pipeline()

    run_early_termination()


if __name__ == "__main__":
    main()