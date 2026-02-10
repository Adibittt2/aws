

from fastapi import FastAPI, UploadFile, File
import boto3
import uvicorn

app = FastAPI()

s3 = boto3.client("s3")
BUCKET_NAME = "s3-demo-708751269284"


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    s3.upload_fileobj(file.file, BUCKET_NAME, file.filename)
    return {"message": "File uploaded successfully"}

@app.get("/files")
def list_files():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    files = [obj for obj in response.get("Contents", [])]
    return {"files": files}



@app.get("/")
def list_files():
    return {"output": "Hello World!"}




if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
