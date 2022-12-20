import os
from http.client import HTTPException
from pathlib import Path

from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse

from app.models import Status
from app.repositories import RequestRepository
from app.services import SongCSVService

app = FastAPI()


@app.get("/result/{request_id}")
def get_result(request_id: int):
    request = RequestRepository().get_request_by_id(request_id)
    if not request:
        return HTTPException(status_code=404, detail="request id not found")
    if request.status == Status.DONE.PROCESSING.value:
        return {
            "code": "success",
            "message": "request is processing",
            "data": {}
        }
    result_file_name = f'result_{request.file_name}'

    def iterfile():
        CHUNK_SIZE = 1024 * 1024
        file = Path(request.file_name).stem
        with open(f'./outputs/{file}_{request.id}/{result_file_name}', 'rb') as f:
            while chunk := f.read(CHUNK_SIZE):
                yield chunk

    headers = {'Content-Disposition': f'attachment; filename="{result_file_name}"'}
    return StreamingResponse(iterfile(), headers=headers, media_type='text/csv')


def process_request(request_id: int, input_file: str, background_tasks: BackgroundTasks):
    service = SongCSVService(request_id, input_file)
    service.process()
    RequestRepository().update_request(request_id, Status.DONE.value)
    background_tasks.add_task(service.clean_up)


@app.post("/upload")
async def upload(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    try:
        path = f'./inputs/{file.filename}'
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            while contents := file.file.read(1024 * 1024):
                f.write(contents)
    except Exception:
        return {
            "code": "success",
            "message": "There was an error uploading the file",
            "data": {}
        }
    finally:
        file.file.close()
    request_id = RequestRepository().create_request(file.filename)
    background_tasks.add_task(process_request, request_id, path, background_tasks)
    return {
        "code": "success",
        "message": "success",
        "data": {
            "requestId": request_id
        }
    }
