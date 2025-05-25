from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import when, lit
from typing import List

def date_diff_fractional(col1: Column,col2: Column) -> Column:
    return (col1.cast("long") - col2.cast("long")) / 86400

def add_time_to_timestamp(tscol: Column,dayscol: Column, additional: "Column | int") -> Column:
    return (tscol.cast("long") + (dayscol * 86400) + (additional * 86400)).cast("timestamp")

def nvl(col:Column, val) -> Column:
    when(col.isNull() == True, lit(val)).otherwise(col)

def select_rename(data_frame: DataFrame, *collection) -> DataFrame:
    col: List[Column] = []
    for item in collection:
        if(type(item) is tuple):
            col.append(data_frame[item[0]].alias(item[1]))
        else:
            col.append(data_frame[item])
    return data_frame.select(*col)

def is_empty(df: 'DataFrame|None') -> bool:
    if isinstance(df, DataFrame):
        return len(df.head(1)) == 0
    else:
        return True