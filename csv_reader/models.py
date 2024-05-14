from pydantic import BaseModel


class DataBaseTableColumn(BaseModel):
    column_name: str
    column_type: str
    primary_key: bool = False
