import csv
from collections.abc import (
    Iterable,
    Iterator,
)
from itertools import islice
from pathlib import Path

from stream_processor.models import (
    EmployeeRecord,
)


def batched_employees(
    employees: Iterable[
        EmployeeRecord
    ],
    batch_size: int,
) -> Iterator[
    list[EmployeeRecord]
]:

    iterator = iter(employees)

    while True:

        batch = list(
            islice(
                iterator,
                batch_size,
            )
        )

        if not batch:
            return

        yield batch


def export_in_batches(
    employees: Iterable[
        EmployeeRecord
    ],
    path: Path,
    batch_size: int,
) -> int:

    batches = 0

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:

        writer = csv.writer(
            file
        )

        writer.writerow(
            EmployeeRecord._fields
        )

        for batch in batched_employees(
            employees,
            batch_size,
        ):

            writer.writerows(
                batch
            )

            batches += 1

    return batches