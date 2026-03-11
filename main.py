from fastapi import FastAPI, status, HTTPException, Depends, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from google.cloud import bigquery

app = FastAPI()

# Dependency method to provide a BigQuery client
# This will be used by endpoints that need database access
def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Return a friendly error message when path parameters are not valid numbers.
    """
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": "All arguments must be valid numbers."}
    )


@app.get("/", status_code=200)
def read_root():
    """
    Health check endpoint.

    Returns:
    - A JSON object confirming the API is running.
    """
    return {"status": "healthy"}


@app.get("/add/{a}/{b}", status_code=200)
def add(a: float, b: float):
    """
    Add two numbers together.

    Parameters:
    - a: First number
    - b: Second number

    Returns:
    - A JSON object containing the operation, inputs, and sum.
    """
    return {
        "operation": "add",
        "a": a,
        "b": b,
        "result": a + b
    }


@app.get("/subtract/{a}/{b}", status_code=200)
def subtract(a: float, b: float):
    """
    Subtract the second number from the first.

    Parameters:
    - a: First number
    - b: Second number

    Returns:
    - A JSON object containing the operation, inputs, and difference.
    """
    return {
        "operation": "subtract",
        "a": a,
        "b": b,
        "result": a - b
    }


@app.get("/multiply/{a}/{b}", status_code=200)
def multiply(a: float, b: float):
    """
    Multiply two numbers together.

    Parameters:
    - a: First number
    - b: Second number

    Returns:
    - A JSON object containing the operation, inputs, and product.
    """
    return {
        "operation": "multiply",
        "a": a,
        "b": b,
        "result": a * b
    }


@app.get("/divide/{a}/{b}", status_code=200)
def divide(a: float, b: float):
    """
    Divide the first number by the second.

    Parameters:
    - a: Dividend
    - b: Divisor

    Returns:
    - A JSON object containing the operation, inputs, and quotient.

    Raises:
    - HTTP 422 error if b is zero.
    """
    if b == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Division by zero is not allowed. Please provide a non-zero value for b."
        )

    return {
        "operation": "divide",
        "a": a,
        "b": b,
        "result": a / b
    }


@app.get("/power/{a}/{b}", status_code=200)
def power(a: float, b: float):
    """
    Raise a to the power of b.

    Parameters:
    - a: Base number
    - b: Exponent

    Returns:
    - A JSON object containing the operation, inputs, and result.
    """
    return {
        "operation": "power",
        "a": a,
        "b": b,
        "result": a ** b
    }


@app.get("/percentage/{part}/{whole}", status_code=200)
def percentage(part: float, whole: float):
    """
    Calculate what percentage the part is of the whole.

    Parameters:
    - part: The portion value
    - whole: The total value

    Returns:
    - A JSON object containing the operation, inputs, and percentage result.

    Raises:
    - HTTP 422 error if whole is zero.
    """
    if whole == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Whole cannot be zero when calculating a percentage."
        )

    return {
        "operation": "percentage",
        "part": part,
        "whole": whole,
        "result": (part / whole) * 100
    }


@app.get("/average/{a}/{b}/{c}", status_code=200)
def average(a: float, b: float, c: float):
    """
    Calculate the average of three numbers.

    Parameters:
    - a: First number
    - b: Second number
    - c: Third number

    Returns:
    - A JSON object containing the operation, inputs, and average.
    """
    return {
        "operation": "average",
        "a": a,
        "b": b,
        "c": c,
        "result": (a + b + c) / 3
    }


@app.get("/dbwritetest", status_code=200)
def dbwritetest(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Writes a simple test row to a BigQuery table.

    Uses the `get_bq_client` dependency method to establish a connection to BigQuery.
    """
    # Define a Python list of objects that will become rows in the database table
    # In this instance, there is only a single object in the list
    row_to_insert = [
        {
            "endpoint": "/dbwritetest",
            "result": "Success",
            "status_code": 200
        }
    ]

    # Use the BigQuery interface to write our data to the table
    # If there are errors, store them in a list called `errors`
    # YOU MUST UPDATE YOUR PROJECT AND DATASET NAME BELOW BEFORE THIS WILL WORK!!!
    errors = bq.insert_rows_json("gen-lang-client-0413676114.calculator.api_logs", row_to_insert)

    # If there were any errors, raise an HTTPException to inform the user
    if errors:
        # Log the full error to your Cloud Run logs for debugging
        print(f"BigQuery Insert Errors: {errors}")

        # Raise an exception to the API user
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "message": "Failed to log data to BigQuery",
                "errors": errors  # Optional: return specific BQ error details
            }
        )

    # If there were NOT any errors, send a friendly response message to the API caller
    return {"message": "Log entry created successfully"}