import os
import re
from typing import Optional, List, Dict, Any

import redshift_connector
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, Query
from pydantic import BaseModel

load_dotenv()

app = FastAPI(
    title="Open Library Data Warehouse API",
    description="REST API for querying and inserting data into Redshift dim/fact tables",
    version="1.0.0"
)

# Chỉ cho phép thao tác trên các bảng này
ALLOWED_TABLES = {
    "dim_author",
    "dim_edition",
    "dim_subject",
    "dim_time",
    "dim_work",
    "fact_book",
    "work_author",
    "work_subject"
}

DIMENSION_TABLES = {
    "dim_author",
    "dim_edition",
    "dim_subject",
    "dim_time",
    "dim_work"
}

FACT_TABLES = {
    "fact_book"
}

BRIDGE_TABLES = {
    "work_author",
    "work_subject"
}


# Body cho insert 1 dòng
class InsertRowRequest(BaseModel):
    data: Dict[str, Any]


# Body cho insert nhiều dòng
class BulkInsertRequest(BaseModel):
    rows: List[Dict[str, Any]]


def check_api_key(x_api_key: Optional[str]):
    expected_key = os.getenv("API_KEY")

    if not expected_key:
        raise HTTPException(
            status_code=500,
            detail="API_KEY is not configured in .env"
        )

    if x_api_key != expected_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )


def get_connection():
    return redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        database=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )


def validate_identifier(name: str) -> str:
    """
    Chỉ cho phép tên bảng/cột dạng chữ, số, dấu gạch dưới.
    Tránh SQL Injection ở identifier.
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid identifier: {name}"
        )
    return name


def validate_table(table_name: str):
    validate_identifier(table_name)

    if table_name not in ALLOWED_TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table_name}' is not allowed"
        )


def rows_to_dict(cursor, rows) -> List[Dict[str, Any]]:
    columns = [desc[0] for desc in cursor.description]

    result = []
    for row in rows:
        item = {}
        for col, value in zip(columns, row):
            item[col] = value
        result.append(item)

    return result


def get_table_columns(table_name: str) -> List[str]:
    """
    Lấy danh sách column thật trong Redshift.
    """
    validate_table(table_name)

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,)
        )

        rows = cursor.fetchall()
        columns = [row[0] for row in rows]

        if not columns:
            raise HTTPException(
                status_code=404,
                detail=f"No columns found for table '{table_name}'"
            )

        return columns

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def validate_insert_columns(table_name: str, data: Dict[str, Any]) -> List[str]:
    """
    Check các cột insert có tồn tại trong bảng không.
    """
    if not data:
        raise HTTPException(status_code=400, detail="Insert data cannot be empty")

    table_columns = set(get_table_columns(table_name))

    input_columns = []
    for col in data.keys():
        validate_identifier(col)

        if col not in table_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{col}' does not exist in table '{table_name}'"
            )

        input_columns.append(col)

    return input_columns


def query_table(table_name: str, limit: int, offset: int):
    validate_table(table_name)

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = f"""
            SELECT *
            FROM public.{table_name}
            LIMIT %s OFFSET %s
        """

        cursor.execute(query, (limit, offset))
        rows = cursor.fetchall()
        data = rows_to_dict(cursor, rows)

        return {
            "table": table_name,
            "limit": limit,
            "offset": offset,
            "row_count": len(data),
            "data": data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def insert_one_row(table_name: str, data: Dict[str, Any]):
    """
    Insert 1 dòng vào bảng bất kỳ trong ALLOWED_TABLES.
    """
    validate_table(table_name)
    columns = validate_insert_columns(table_name, data)

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        col_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        values = [data[col] for col in columns]

        query = f"""
            INSERT INTO public.{table_name} ({col_sql})
            VALUES ({placeholders})
        """

        cursor.execute(query, values)
        conn.commit()

        return {
            "message": "Insert successfully",
            "table": table_name,
            "inserted_rows": 1,
            "data": data
        }

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def insert_many_rows(table_name: str, rows: List[Dict[str, Any]]):
    """
    Bulk insert nhiều dòng.
    Yêu cầu tất cả row có cùng bộ column.
    """
    validate_table(table_name)

    if not rows:
        raise HTTPException(status_code=400, detail="Rows cannot be empty")

    first_columns = list(rows[0].keys())
    validate_insert_columns(table_name, rows[0])

    for idx, row in enumerate(rows):
        if list(row.keys()) != first_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Row {idx} has different columns. All rows must have the same columns."
            )
        validate_insert_columns(table_name, row)

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        col_sql = ", ".join(first_columns)
        placeholders = ", ".join(["%s"] * len(first_columns))

        query = f"""
            INSERT INTO public.{table_name} ({col_sql})
            VALUES ({placeholders})
        """

        values = []
        for row in rows:
            values.append([row[col] for col in first_columns])

        cursor.executemany(query, values)
        conn.commit()

        return {
            "message": "Bulk insert successfully",
            "table": table_name,
            "inserted_rows": len(rows)
        }

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.get("/")
def health_check():
    return {
        "status": "ok",
        "message": "Open Library Redshift API is running"
    }


@app.get("/tables")
def get_available_tables(
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)

    return {
        "all_tables": sorted(list(ALLOWED_TABLES)),
        "dimension_tables": sorted(list(DIMENSION_TABLES)),
        "fact_tables": sorted(list(FACT_TABLES)),
        "bridge_tables": sorted(list(BRIDGE_TABLES))
    }


@app.get("/table/{table_name}/columns")
def get_columns(
    table_name: str,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    validate_table(table_name)

    return {
        "table": table_name,
        "columns": get_table_columns(table_name)
    }


@app.get("/table/{table_name}")
def get_table_data(
    table_name: str,
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return query_table(table_name, limit, offset)


@app.get("/table/{table_name}/count")
def count_table_rows(
    table_name: str,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    validate_table(table_name)

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = f"""
            SELECT COUNT(*)
            FROM public.{table_name}
        """

        cursor.execute(query)
        total_rows = cursor.fetchone()[0]

        return {
            "table": table_name,
            "total_rows": total_rows
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# GENERIC INSERT ENDPOINTS
@app.post("/table/{table_name}")
def insert_table_row(
    table_name: str,
    request: InsertRowRequest,
    x_api_key: Optional[str] = Header(None)
):
    """
    Insert 1 dòng vào bảng bất kỳ.

    Body:
    {
      "data": {
        "column_1": "value",
        "column_2": 123
      }
    }
    """
    check_api_key(x_api_key)
    return insert_one_row(table_name, request.data)


@app.post("/table/{table_name}/bulk")
def bulk_insert_table_rows(
    table_name: str,
    request: BulkInsertRequest,
    x_api_key: Optional[str] = Header(None)
):
    """
    Insert nhiều dòng vào bảng bất kỳ.

    Body:
    {
      "rows": [
        {"column_1": "value1", "column_2": 123},
        {"column_1": "value2", "column_2": 456}
      ]
    }
    """
    check_api_key(x_api_key)
    return insert_many_rows(table_name, request.rows)

# GROUP GET ENDPOINTS
@app.get("/dimensions")
def get_all_dimensions(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)

    result = {}
    for table_name in sorted(DIMENSION_TABLES):
        result[table_name] = query_table(table_name, limit, offset)

    return result


@app.get("/facts")
def get_all_facts(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)

    result = {}
    for table_name in sorted(FACT_TABLES):
        result[table_name] = query_table(table_name, limit, offset)

    return result


@app.get("/bridges")
def get_all_bridge_tables(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)

    result = {}
    for table_name in sorted(BRIDGE_TABLES):
        result[table_name] = query_table(table_name, limit, offset)

    return result

# SPECIFIC GET ENDPOINTS
@app.get("/authors")
def get_authors(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return query_table("dim_author", limit, offset)


@app.get("/editions")
def get_editions(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return query_table("dim_edition", limit, offset)


@app.get("/subjects")
def get_subjects(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return query_table("dim_subject", limit, offset)


@app.get("/times")
def get_times(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return query_table("dim_time", limit, offset)


@app.get("/works")
def get_works(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return query_table("dim_work", limit, offset)


@app.get("/books")
def get_books(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return query_table("fact_book", limit, offset)


@app.get("/work-authors")
def get_work_authors(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return query_table("work_author", limit, offset)


@app.get("/work-subjects")
def get_work_subjects(
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return query_table("work_subject", limit, offset)

# SPECIFIC INSERT ENDPOINTS
@app.post("/authors")
def insert_author(
    request: InsertRowRequest,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return insert_one_row("dim_author", request.data)


@app.post("/editions")
def insert_edition(
    request: InsertRowRequest,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return insert_one_row("dim_edition", request.data)


@app.post("/subjects")
def insert_subject(
    request: InsertRowRequest,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return insert_one_row("dim_subject", request.data)


@app.post("/times")
def insert_time(
    request: InsertRowRequest,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return insert_one_row("dim_time", request.data)


@app.post("/works")
def insert_work(
    request: InsertRowRequest,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return insert_one_row("dim_work", request.data)


@app.post("/books")
def insert_book(
    request: InsertRowRequest,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return insert_one_row("fact_book", request.data)


@app.post("/work-authors")
def insert_work_author(
    request: InsertRowRequest,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return insert_one_row("work_author", request.data)


@app.post("/work-subjects")
def insert_work_subject(
    request: InsertRowRequest,
    x_api_key: Optional[str] = Header(None)
):
    check_api_key(x_api_key)
    return insert_one_row("work_subject", request.data)