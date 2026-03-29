from datetime import datetime, timedelta # represents a change between two date times
from pydantic import BaseModel


class KLinesQuery(BaseModel):
    start: datetime
    end: datetime


def date_range_split(start_date: datetime, end_date: datetime, chunk_range: timedelta) -> list[KLinesQuery]:
    """
    Chunks a start_date, end_date wrt to the chunk_range
    """
    chunks: list[KLinesQuery] = []
    curr_start_date: datetime = start_date
    while curr_start_date < end_date:
        curr_end_date: datetime = min(curr_start_date + chunk_range, end_date)
        chunks.append(KLinesQuery(start=curr_start_date, end=curr_end_date))
        curr_start_date = curr_end_date
    return chunks



if __name__ == "__main__":
    start = datetime(2025, 1, 1)
    end = datetime(2025, 2, 1)
    chunks = date_range_split(start, end, timedelta(hours=10))
    print(chunks)
