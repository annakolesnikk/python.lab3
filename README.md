# Stream Processor

Laboratory work No. 3 for the course "Professional Python".

## Description

Streaming pipeline for processing employee data (variant 6):
streaming validation, filtering by department and salary,
streaming average salary, highest paid employee, payroll fund,
batch export and counting employees by department.

## Requirements

Python 3.11+

## Installation

```
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

## Run

```
python -m stream_processor.main
python -m stream_processor.experiment
```

## Project structure

- `data/input.csv` — input dataset
- `src/stream_processor/models.py` — EmployeeRecord model
- `src/stream_processor/iterators.py` — custom iterable and iterator
- `src/stream_processor/readers.py` — streaming reader
- `src/stream_processor/parsers.py` — cleaning and CSV parsing
- `src/stream_processor/filters.py` — validation and filtering
- `src/stream_processor/transformations.py` — transformation stage
- `src/stream_processor/batches.py` — batching and batch export
- `src/stream_processor/analytics.py` — streaming aggregation
- `src/stream_processor/pipeline.py` — pipeline construction
- `src/stream_processor/experiment.py` — eager vs lazy experiment
- `src/stream_processor/main.py` — application entry point

## Author

Student: Anna Kolesnik
Group: FEP-33