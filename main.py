
from contextlib import redirect_stderr, redirect_stdout
import io
import time
from fastapi import BackgroundTasks, FastAPI, Depends, Form, UploadFile, File, HTTPException,Request
from typing import List
from fastapi.middleware.cors import CORSMiddleware
import nbformat
from sqlalchemy.orm import Session
from sqlalchemy import text
from .services.activity.copy.main import getFileSource1, uploadFiles , copyData  ,getDataWithFormatChange
from .database import get_db_1, get_db_2, get_db_3
from pydantic import BaseModel
import logging
import httpx
from enum import Enum
from pydantic import constr
import traceback
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db_by_id(id: int):
    if id == 1:
        return get_db_1
    else:
        return get_db_2


@app.get("/getfileSource1/{id}")
def getupload_fileSource1(id: int, db1: Session = Depends(get_db_2),db2: Session = Depends(get_db_3)):
    if(id==1):
        file_list = getFileSource1( db1)
    else:
        file_list = getFileSource1( db2)
    return file_list


@app.post("/upload-file/{id}")
def upload_file(id: int, file: UploadFile = File(...), db2: Session = Depends(get_db_2), db3: Session = Depends(get_db_3)):
    if id == 1:
        file = uploadFiles(file, db2)
    else:
        file = uploadFiles(file, db3)
        
    return file

class OperationType(str, Enum):
    one_time = "one_time"
    schedule = "schedule"
    event_change = "event_change"
    tumbling_window = "tumbling_window"

class CopyData(BaseModel):                         
    source: int
    filename: str
    filetype: str
    content: str
    
@app.post("/copy-data/")
def copy_data(
    source: int = Form(...),  
    filename: str = Form(...), 
    filetype: str = Form(...),  
    file: UploadFile = File(...),  
    db1: Session = Depends(get_db_2), 
    db2: Session = Depends(get_db_3)
):
    # Choose the database based on the source
    db = db1 if source == 1 else db2

    # Read the file content
    file_content = file.file.read()

    # Process the file data and store it in the database
    result = copyData(filename, file_content, filetype, db)

    return result


class FormatFile(BaseModel):
    id: int                              
    source: int
    format: str
    fileName: str 
@app.post("/FileConvert/")
async def file_convert(body: FormatFile, db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3)):
    try:
        res = getDataWithFormatChange(body,db1)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def get_temp_data(db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3)):
    try:
        files_db1 = db1.execute(text("SELECT * FROM FileStorage")).fetchall()
        temp_data = []
        print(temp_data)
        for row in files_db1:
            check_exist = db2.execute(text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"), {"filename": row.filename}).fetchone()[0]
            if check_exist == 0:
                temp_data.append(row)
        return temp_data
    except Exception as e:
        logger.error("Error in get_temp_data: %s", str(e))
        return []
def copy_data_task(temp_data: List, interval, db2: Session = Depends(get_db_2)):
    try:
        print("copy_data_task")
        if temp_data:
            for row in temp_data:
                db2.execute(text("""
                    INSERT INTO FileStorage (filename, content, filetype)
                    VALUES (:filename, :content, :filetype)
                """), {
                    "filename": row.filename,
                    "content": row.content,
                    "filetype": row.filetype
                })
                logger.info("Record copied from db1 to db2: %s", row.filename)
            db2.commit()
        time.sleep(interval)
    except Exception as e:
        db2.rollback()
        logger.error("Error in copy_data_task: %s", str(e))

@app.post("/startCopyDataTask/", response_model=None)
def start_copy_data_task(background_tasks: BackgroundTasks, db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3), interval: int = 1000):
    temp_data = get_temp_data(db1, db2)

    background_tasks.add_task(copy_data_task, temp_data, interval, db2)
    return {"message": "Scheduled copy data task started"}



class ExecuteApiRequest(BaseModel):
    title: str
    url: str
    method: str = "GET"
    headers: dict = {}
    data: dict = None
@app.post("/executeApi")
async def execute_api(body: ExecuteApiRequest):
    try:
        # Extract data from the request body
        url = body.url
        method = body.method.upper()  # Ensure method is uppercase
        headers = body.headers
        data = body.data

        # Validate the HTTP method
        if method not in ["GET", "POST", "PUT", "DELETE"]:
            raise HTTPException(status_code=405, detail="HTTP method not supported")

        # Create an HTTP client to execute the request
        async with httpx.AsyncClient() as client:
            if method == 'GET':
                response = await client.get(url, headers=headers)
            elif method == 'POST':
                response = await client.post(url, headers=headers, json=data)
            elif method == 'PUT':
                response = await client.put(url, headers=headers, json=data)
            elif method == 'DELETE':
                response = await client.delete(url, headers=headers)
                # Prepare the result
        print(response.json())
        result = {
                    "filename": body.title.split('.')[0] + f".json",
                    "content":  response.json(),
                    "filetype": 'application/json'
        }
        return {"message": "Data retrieved successfully", "data": result}

    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Request failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    


def run_code_cell(code, cell_position):
    try:
        # Create a string buffer to capture output
        output_buffer = io.StringIO()
        error_buffer = io.StringIO()
        
        # Redirect stdout and stderr to the buffers
        with redirect_stdout(output_buffer), redirect_stderr(error_buffer):
            exec(code)
        
        # Retrieve the output and error messages
        output = output_buffer.getvalue()
        errors = error_buffer.getvalue()
        
        if errors:
            raise Exception(errors)
        
        return {
            "success": True,
            "cell_position": cell_position,
            "output": output
        }
    
    except SyntaxError as e:
        # Handle syntax errors explicitly
        return {
            "success": False,
            "cell_position": cell_position,
            "error": {
                "type": "SyntaxError",
                "message": str(e),
                "traceback": f"Line {e.lineno}: {e.text.strip()}",
            }
        }
    
    except Exception as e:
        # Handle all other exceptions and capture traceback as string
        error_traceback = traceback.format_exc()  # Format the full traceback as a string
        return {
            "success": False,
            "cell_position": cell_position,
            "error": {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": error_traceback,  # Store traceback as a string
            }
        }

@app.post("/compileNotebook/")
async def compile_notebook(file: UploadFile = File(...)):
    try:
        # Ensure the uploaded file is a .ipynb file
        if not file.filename.endswith(".ipynb"):
            raise HTTPException(status_code=400, detail="File must be a Jupyter Notebook (.ipynb)")

        # Read the notebook
        content = await file.read()
        notebook = nbformat.reads(content.decode("utf-8"), as_version=nbformat.NO_CONVERT)

        if notebook["nbformat"] < 4:
            raise HTTPException(status_code=400, detail="Unsupported notebook format. Use a format >= 4.")
        
        # Extract and run code cells
        errors = []
        last_successful_cell = None
        
        for index, cell in enumerate(notebook.cells):
            if cell.cell_type == "code":
                code = cell.source 
                result = run_code_cell(code, index + 1)  

                if not result["success"]:
                    # Return structured error details along with the last successful cell
                    return {
                        "message": "Error encountered during execution",
                        "last_successful_cell": last_successful_cell,
                        "error_details": result["error"]
                    }
                else:
                    last_successful_cell = result

        # If no errors, return the last successful cell
        return {
            "message": "Notebook ran successfully, no errors encountered.",
            "last_successful_cell": last_successful_cell
        }

    except Exception as e:
        error_traceback = traceback.format_exc()
        # Return structured error response
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Error processing notebook",
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": error_traceback
            }
        )