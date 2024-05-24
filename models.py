from pydantic import BaseModel
from typing import Optional
from fastapi import Form


class Bucket(BaseModel):
    bucket_name: str
    region_name: str


class BucketFile(Bucket):
    file_name: str


class Base(BaseModel):
    name: str
    point: Optional[float] = None
    is_accepted: Optional[bool] = False


def checker(data: str = Form(...)):
    return Base.model_validate_json(data)
