from stream_processor.models import EmployeeRecord


class EmployeeCollection:

    def __init__(
        self,
        employees: list[EmployeeRecord],
    ) -> None:

        self.employees = employees

    def __iter__(self):

        return EmployeeIterator(
            self.employees,
        )


class EmployeeIterator:

    def __init__(
        self,
        employees: list[EmployeeRecord],
    ) -> None:

        self.employees = employees
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self) -> EmployeeRecord:

        if self.index >= len(self.employees):
            raise StopIteration

        employee = self.employees[self.index]

        self.index += 1

        return employee