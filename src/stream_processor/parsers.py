import csv
from collections.abc import (
    Iterable,
    Iterator,
)


def clean_lines(
    lines: Iterable[str],
) -> Iterator[str]:

    for line in lines:

        value = line.strip()

        if value:
            yield value


def parse_csv_rows(
    lines: Iterable[str],
) -> Iterator[dict[str, str]]:

    reader = csv.DictReader(
        lines
    )

    yield from reader