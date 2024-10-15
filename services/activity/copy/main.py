from io import StringIO
import os
from tempfile import NamedTemporaryFile
from turtle import pd
from fastapi import Depends, Form, HTTPException,UploadFile,File
import httpx
from sqlalchemy import text
import sys
sys.path.append('C:\\Users\\APRIYADARS1\\OneDrive - Rockwell Automation, Inc\\Desktop\\Project\\ADF_Backend')
from database import get_db_1, get_db_2,get_db_3
from sqlalchemy.orm import Session
import base64
import csv
import json
import pusher
from fastapi import BackgroundTasks
import time

pusher = pusher.Pusher(
  app_id='1879605',
  key='aeeb90d987e5e72bddbe',
  secret='543074e9650b9560798e',
  cluster='ap2',
  ssl=True
)

def get_db_by_id(id: int):
    if id == 1:
        return get_db_1
    else:
        return get_db_2
def getFileSource1(db: Session = Depends(get_db_1)):
    try:
        pusher.trigger("logs-channel", "log-event", {
        "message": f"Running node: Fetching data from source location",
        })
        files = db.execute(text("SELECT * FROM FileStorage")).fetchall()
        file_list = []
        for row in files:
            file_dict = dict(row._mapping)
            file_dict['content'] = base64.b64encode(file_dict['content']).decode('utf-8')
            file_list.append(file_dict)
        pusher.trigger("logs-channel", "log-event", {
        "message": f"Running node: Fetched data from source location",
        })
        return file_list
    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
        "message": f"Error: {str(e)}",
        })
        raise HTTPException(status_code=500, detail=str(e))
    

ALLOWED_FILE_TYPES = ["text/csv", "application/json", "text/plain", "application/xml", "text/xml"]

def uploadFiles(file: UploadFile = File(...), db: Session = Depends(get_db_1)):
    try:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Upload File Activity",
            "message": "Uploading file...",
            "status": "success"
        })
        file_type = file.content_type
        if file_type not in ALLOWED_FILE_TYPES:
            pusher.trigger("logs-channel", "log-event", {
                "label":"Upload File Activity",
                "message": f"Invalid file type '{file_type}'. Only CSV, JSON, TXT, and XML files are allowed.",
                "status": "error"
            })
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid file type '{file_type}'. Only CSV, JSON, TXT, and XML files are allowed."
            )

        file_content = file.file.read()
        
        countDuplicate = db.execute(
            text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"), 
            {"filename": file.filename}
        ).fetchone()[0]
        
        if countDuplicate > 0:
            pusher.trigger("logs-channel", "log-event", {
                "label":"Upload File Activity",
                "message": "File already exists",
                "status": "error"
            })
            raise HTTPException(status_code=400, detail="File already exists")
        
        if checkFileCurrupt(file):
            raise HTTPException(status_code=400, detail="File is corrupted or unreadable")
        

        db.execute(text("""
        IF OBJECT_ID('FileStorage', 'U') IS NULL
        BEGIN
            CREATE TABLE FileStorage (
                id INT IDENTITY PRIMARY KEY,
                filename VARCHAR(255),
                content VARBINARY(MAX),
                filetype VARCHAR(255)
            );
        END
        INSERT INTO FileStorage (filename, content, filetype) VALUES (:filename, :content, :filetype)
        """), {"filename": file.filename, "content": file_content, "filetype": file_type})
        
        db.commit()
        pusher.trigger("logs-channel", "log-event", {
            "label":"Upload File Activity",
            "message": "File uploaded successfully",
            "status": "success"
        })
        return {"message": "File uploaded successfully"}
    
    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Upload File Activity",
            "message": f"Error: {str(e)}"
        })
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    

def checkFileCurrupt(file: UploadFile = File(...)) -> bool:
    try:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Currupt File Activity",
            "message": "Checking file integrity...",
            "status": "success"
        })
        # Ensure the file pointer is at the beginning
        file.file.seek(0)  
        file_content = file.file.read()
        # Reset the file pointer to the beginning after reading
        file.file.seek(0)
        # If no exception, the file is not corrupted  
        pusher.trigger("logs-channel", "log-event", {
            "label":"Currupt File Activity",
            "message": "File is not corrupted and readable",
            "status": "success"
        })
        return False  
    except Exception:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Currupt File Activity",
            "message": "File is corrupted or unreadable",
            "status": "error"
        })
        return True 



import xml.etree.ElementTree as ET




def copyDataSchedule( interval: int, background_tasks: BackgroundTasks, db1: Session = Depends(get_db_2), db2: Session = Depends(get_db_3)):
    pusher.trigger("logs-channel", "log-event", {
        "message": "Scheduled copy data task started",
        "status": "success"
    })
    def copy_data_task():
        while True:
            try:
                temp_data = db1.execute(text("SELECT * FROM FileStorage")).fetchall()
                if temp_data:
                    for row in temp_data:
                        checkExist = db2.execute(text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"), {"filename": row.filename}).fetchone()[0]
                        if checkExist > 0:
                            raise HTTPException(status_code=400, detail="File already exists")
                        db2.execute(text("""
                            INSERT INTO FileStorage (filename, content, filetype)
                            VALUES (:filename, :content, :filetype)
                        """), {
                            "filename": row.filename,
                            "content": row.content,
                            "filetype": row.filetype
                        })
                    db2.commit()
                time.sleep(interval)
            except Exception as e:
                pusher.trigger("logs-channel", "log-event", {
                    "message": f"Error: {str(e)}",
                    "status": "error"
                })
                db2.rollback()
                raise HTTPException(status_code=400, detail=str(e))

    background_tasks.add_task(copy_data_task)
   
    return {"message": "Scheduled copy data task started"}


def csv_to_json(csv_content):
    """Convert CSV content to JSON format."""
    pusher.trigger("logs-channel", "log-event", {
        "message": "Converting CSV to JSON",
        "status": "success"
    })
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    json_data = json.dumps([row for row in reader])
    pusher.trigger("logs-channel", "log-event", {
        "message": "CSV converted to JSON",
        "status": "success"
    })
    return json_data

def json_to_csv(json_content):
    """Convert JSON content to CSV format."""
    pusher.trigger("logs-channel", "log-event", {
        "message": "Converting JSON to CSV",
        "status": "success"
    })
    json_data = json.loads(json_content)
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=json_data[0].keys())
    writer.writeheader()
    writer.writerows(json_data)
    pusher.trigger("logs-channel", "log-event", {
        "message": "Converted JSON to CSV",
        "status": "success"
    })
    return output.getvalue()

def copy_data(
    source: int = Form(...),  
    filename: str = Form(...),  
    filetype: str = Form(...), 
    file: UploadFile = File(...), 
    db1: Session = Depends(get_db_2), 
    db2: Session = Depends(get_db_3)
):
    pusher.trigger("logs-channel", "log-event", {
        "message": "Copying data..."
    })
    # Choose the database based on the source
    db = db1 if source == 1 else db2

    # Read the file content
    file_content = file.file.read()

    # Process the file data and store it in the database
    result = copyData(filename, file_content, filetype, db)
    pusher.trigger("logs-channel", "log-event", {
        "message": "Data copied successfully"
    })

    return result

def copyData(filename: str, content: bytes, filetype: str, db: Session):
    try:
        # Check if the file already exists in the database
        pusher.trigger("logs-channel", "log-event", {
            "label": "Copy Data Activity",
            "message": "Checking if file already exists...",
            "status": "success"
        })
        checkExist = db.execute(
            text("SELECT COUNT(*) FROM FileStorage WHERE filename = :filename"),
            {"filename": filename}
        ).fetchone()[0]

        if checkExist > 0:
            pusher.trigger("logs-channel", "log-event", {
                "label": "Copy Data Activity",
                "message": "File already exists",
                "status": "error"
            })
            raise HTTPException(status_code=400, detail="File already exists")
        pusher.trigger("logs-channel", "log-event", {
            "label": "Copy Data Activity",
            "message": f"Inserting {filename} data to new location...",
            "status": "success"
        })
        # Insert the file data into the database
        db.execute(text("""
            INSERT INTO FileStorage (filename, content, filetype)
            VALUES (:filename, :content, :filetype)
        """), {
            "filename": filename,
            "content": content,
            "filetype": filetype
        })
        pusher.trigger("logs-channel", "log-event", {
            "label": "Copy Data Activity",
            "message": f"Data copied successfully ",
            "status": "success"
        })

        db.commit()

        return {"message": f"Data copied successfully for filename {filename}"}

    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "label": "Copy Data Activity",
            "message": f"Error: {str(e)}",
            "status": "error"
        })
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    



def csv_to_json(csv_content):
    """Convert CSV content to JSON format."""
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    return json.dumps([row for row in reader])

def csv_to_txt(csv_content):
    """Convert CSV content to plain text."""
    return csv_content  # CSV is already in a textual format, so we return it as-is

def csv_to_xml(csv_content):
    """Convert CSV content to XML format."""
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    root = ET.Element("root")
    for row in reader:
        item = ET.SubElement(root, "item")
        for key, value in row.items():
            child = ET.SubElement(item, key)
            child.text = value
    return ET.tostring(root).decode()

def json_to_csv(json_content):
    """Convert JSON content to CSV format."""
    json_data = json.loads(json_content)
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=json_data[0].keys())
    writer.writeheader()
    writer.writerows(json_data)
    return output.getvalue()

def json_to_txt(json_content):
    """Convert JSON content to plain text."""
    return json_content  # JSON is already text format

def json_to_xml(json_content):
    """Convert JSON content to XML format."""
    json_data = json.loads(json_content)
    root = ET.Element("root")
    for item in json_data:
        item_elem = ET.SubElement(root, "item")
        for key, value in item.items():
            child = ET.SubElement(item_elem, key)
            child.text = str(value)
    return ET.tostring(root).decode()

def txt_to_json(txt_content):
    """Convert plain text content to JSON format."""
    return json.dumps({"content": txt_content})

def txt_to_csv(txt_content):
    """Convert plain text content to CSV format."""
    output = StringIO()
    output.write(txt_content)
    return output.getvalue()

def txt_to_xml(txt_content):
    """Convert plain text content to XML format."""
    root = ET.Element("root")
    content_elem = ET.SubElement(root, "content")
    content_elem.text = txt_content
    return ET.tostring(root).decode()

def xml_to_json(xml_content):
    """Convert XML content to JSON format."""
    root = ET.fromstring(xml_content)
    result = []
    for item in root.findall("item"):
        item_data = {child.tag: child.text for child in item}
        result.append(item_data)
    return json.dumps(result)

def xml_to_csv(xml_content):
    """Convert XML content to CSV format."""
    json_data = json.loads(xml_to_json(xml_content))
    return json_to_csv(json.dumps(json_data))

def xml_to_txt(xml_content):
    """Convert XML content to plain text."""
    root = ET.fromstring(xml_content)
    output = []
    for item in root.findall("item"):
        for child in item:
            output.append(f"{child.tag}: {child.text}")
    return "\n".join(output)

# Main function for fetching data and converting formats


def getDataWithFormatChange(body, db1: Session = Depends(get_db_2)):
    try:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Format Activity",
            "message": f"Running node: Fetching data from source database for id {body.id}",
            "status": "success"
        })
        # Fetch data from the source database
        temp_data = db1.execute(text("SELECT * FROM FileStorage WHERE id = :id").bindparams(id=body.id)).fetchall()

        if not temp_data:
            pusher.trigger("logs-channel", "log-event", {
                "label":"Format Activity",
                "message": f"No data found for id {body.id}",
                "status": "error"
            })
            raise HTTPException(status_code=404, detail="No data found for the given id")

        result = []
        pusher.trigger("logs-channel", "log-event", {
            "label":"Format Activity",
            "message": f"Data fetched successfully for id {body.id}",
            "status": "success"
        })
        # Iterate over the fetched rows and perform format conversion if needed
        for row in temp_data:
            content = row.content
            filetype = row.filetype
            if isinstance(content, bytes):
                content=content.decode('utf-8')
            target_format =body.format
            # Check if format conversion is needed
                # Convert based on current filetype
            if filetype == 'text/csv':
                if target_format == 'json':
                    content = csv_to_json(content)
                elif target_format == 'txt':
                    content = csv_to_txt(content)
                elif target_format == 'xml':
                    content = csv_to_xml(content)
            elif filetype == 'application/json':
                if target_format == 'csv':
                    content = json_to_csv(content)
                elif target_format == 'txt':
                    content = json_to_txt(content)
                elif target_format == 'xml':
                    content = json_to_xml(content)
            elif filetype == 'text/plain':
                if target_format == 'csv':
                    content = txt_to_csv(row.content)
                elif target_format == 'json':
                    content = txt_to_json(row.content)
                elif target_format == 'xml':
                    content = txt_to_xml(row.content)
            elif filetype == "application/xml" or filetype == "text/xml":
                if target_format == 'json':
                    content = xml_to_json(row.content)
                elif target_format == 'txt':
                    content = xml_to_txt(row.content)
                elif target_format == 'csv':
                    content = xml_to_csv(row.content)
            else:
                pusher.trigger("logs-channel", "log-event", {
                    "label":"Format Activity",
                    "message": f"Unsupported format conversion",
                    "status": "error"})
                raise HTTPException(status_code=400, detail="Unsupported format conversion")
            

            with NamedTemporaryFile(delete=False, suffix=f".{body.format}") as temp_file:
                temp_file.write(content.encode('utf-8'))
                temp_file_path = temp_file.name

            # Read the file content
            with open(temp_file_path, 'rb') as file:
                file_content = file.read()
              
            # Clean up the temporary file
            os.remove(temp_file_path)
            pusher.trigger("logs-channel", "log-event", {
                "label":"Format Activity",
                "message": f"Converted data to {body.format} format",
                "status": "success"
            })

            # Prepare the result
            result.append({
                "filename": body.fileName.split('.')[0] + f".{body.format}",
                "content": content,
                "filetype": body.format == 'xml' and 'application/xml' or body.format == 'json' and 'application/json' or body.format == 'csv' and 'text/csv' or 'text/plain',
                "content1": file_content
            })

        return {"message": f"Data retrieved successfully for id {body.id}", "data": result}

    except Exception as e:
        pusher.trigger("logs-channel", "log-event", {
            "label":"Format Activity",
            "message": f"Error: {str(e)}",
            "status": "error"
        })
        raise HTTPException(status_code=400, detail=str(e))
    

