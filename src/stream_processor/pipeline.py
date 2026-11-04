from collections import Counter
from pathlib import Path

from stream_processor.filters import (
    filter_by_department,
    filter_by_salary,
    validate_employees,
)
from stream_processor.parsers import (
    clean_lines,
    parse_csv_rows,
)
from stream_processor.readers import (
    read_lines,
)
from stream_processor.transformations import (
    normalize_names,
)


def build_pipeline(
    path: Path,
    department: str | None = None,
    minimum_salary: float = 0.0,
    invalid: Counter | None = None,
):

    lines = read_lines(
        path
    )

    cleaned = clean_lines(
        lines
    )

    rows = parse_csv_rows(
        cleaned
    )

    valid = validate_employees(
        rows,
        invalid,
    )

    by_department = filter_by_department(
        valid,
        department,
    )

    by_salary = filter_by_salary(
        by_department,
        minimum_salary,
    )

    normalized = normalize_names(
        by_salary
    )

    return normalized