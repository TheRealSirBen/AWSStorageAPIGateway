from fastapi import FastAPI
from fastapi import Response
from fastapi import status
from fastapi import UploadFile
from fastapi import File
from fastapi import Form
from fastapi.responses import FileResponse

from models import Bucket

from boto3 import Session
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.exceptions import EndpointConnectionError

from database import ping_db
from database import insert_record
from database import get_record

from helper import generate_temporary_filename
from helper import prepare_file_name
from helper import encrypt
from helper import decrypt
from helper import file_or_dir
from helper import get_file_media_type

from os import environ
from os import makedirs

from typing import Annotated
from bson import ObjectId
from logging import info
from uuid import uuid4

aws_session = Session(
    aws_access_key_id=environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=environ.get('AWS_SECRET_ACCESS_KEY')
)

ENCRYPTION_KEY = environ.get('FILE_ENCRYPTION_KEY')
COLLECTION_NAME = environ.get('MONGO_DB_APP_COLLECTION')


async def aws_s3_session(region: str) -> BaseClient:
    return aws_session.client('s3', region_name=region)


async def get_response_status(response: dict):
    response_meta_data: dict = response.get('ResponseMetadata')
    return response_meta_data.get('HTTPStatusCode')


async def check_bucket(bucket_name: str, s3_session):
    """
    :param bucket_name:
    :param s3_session:
    :return: [, , 200|206|404|400]
    """

    info('Bucket check start')
    message: str = str()
    try:
        response: dict = s3_session.head_bucket(Bucket=bucket_name)
        response_status = await get_response_status(response)

        # When response status is 200
        if response_status == 200:
            info('Bucket {} exists'.format(bucket_name))
            return True, status.HTTP_200_OK, message

        # When none of the requests pass
        message = 'Bucket check failed'
        info(message)
        return False, status.HTTP_206_PARTIAL_CONTENT, message

    # When Bucket does not exist
    except ClientError:
        message = 'Bucket does not exist'
        info(message)
        return False, status.HTTP_404_NOT_FOUND, message

    # When Region is incorrect
    except EndpointConnectionError:
        message = 'Region does not exist'
        info(message)
        return False, status.HTTP_400_BAD_REQUEST, message


async def list_buckets(s3_session):
    message: str = str()
    buckets: list = list()
    try:
        response: dict = s3_session.list_buckets()
        response_status = await get_response_status(response)
        buckets = response.get('Buckets')
        return response_status, buckets, message

    except ClientError:
        message = 'Invalid region for user'
        info(message)
        return status.HTTP_400_BAD_REQUEST, buckets, message


async def bucket_contents(bucket_name: str, s3_session):
    response: dict = s3_session.list_objects_v2(Bucket=bucket_name)
    response_status = await get_response_status(response)

    if response_status == 200:
        contents_list: list[dict] = response.get('Contents')
        contents: list[dict] = list()

        # When bucket contains file or dir objects
        if contents_list:
            for content in contents_list:
                item = content.get('Key')
                item_type = file_or_dir(item)
                contents.append({'name': item, 'type': item_type})

        return contents


async def check_bucket_file(bucket_name: str, file_name: str, s3_session):
    _check_bucket, _check_bucket_status, _check_bucket_message = await check_bucket(bucket_name, s3_session)

    # When bucket exists
    if _check_bucket_status == 200:
        contents_data: list[dict] = await bucket_contents(bucket_name, s3_session)
        file_in_list = [content for content in contents_data if content.get('name') == file_name]

        message = str()
        # When file has been found
        if len(file_in_list) > 0:
            message = 'file {} exists'.format(file_name)
            return True, status.HTTP_200_OK, message

        # When file has not been found
        return False, status.HTTP_404_NOT_FOUND, message

    # When bucket does not exist
    if _check_bucket_status == 404:
        return _check_bucket, status.HTTP_428_PRECONDITION_REQUIRED, _check_bucket_message


async def upload_to_bucket(bucket_name: str, file_path, file_name: str, app_name: str, s3_session):
    # Create file bytes
    with open(file_path, 'rb') as file:
        file_data = file.read()
    file.close()

    info('Bucket name: {}'.format(bucket_name))

    response: dict = s3_session.put_object(
        Body=file_data, Bucket=bucket_name, Key=file_name
    )
    response_status = await get_response_status(response)

    # When file upload is successful
    new_file_record_id: str = str()
    if response_status == 200:
        new_file_record = {'file_name': file_name}
        record_id = await insert_record(app_name, COLLECTION_NAME, new_file_record)
        new_file_record_id = encrypt(record_id, ENCRYPTION_KEY)
        return new_file_record_id, response_status

    return new_file_record_id, response_status


async def get_file_name_by_id(file_id: str, app_name: str):
    decrypted_file_id = decrypt(file_id, ENCRYPTION_KEY)
    info(decrypted_file_id)
    filter_query = {'_id': ObjectId(decrypted_file_id)}
    record_data = await get_record(app_name, COLLECTION_NAME, filter_query)

    if record_data:
        return record_data.get('file_name')


async def download_from_bucket(bucket_name: str, file_name: str, s3_session) -> tuple[str, str, str]:
    temp_dir = environ.get('TEMP_DIR')
    _, file_extension = prepare_file_name(file_name)
    local_file_name = '{}.{}'.format(str(uuid4()), file_extension)
    local_file_path = '{}/{}'.format(temp_dir, local_file_name)
    s3_session.download_file(bucket_name, file_name, local_file_path)
    return local_file_path, local_file_name, file_extension


#
app = FastAPI()


@app.get("/")
async def root():
    await ping_db()
    return {"message": "Welcome to the AWS Storage Gateway Endpoint"}


@app.get("/ping/{bucket_name}")
async def ping_bucket(bucket_name: str, region_name: str, resp: Response):
    s3_session = await aws_s3_session(region_name)

    # When user requests all buckets
    if bucket_name == 'all':
        buckets_status, buckets_list, buckets_message = await list_buckets(s3_session)

        # When retrieval was unsuccessful
        if buckets_status == 400:
            resp.status_code = buckets_status
            return {'message': buckets_message}

        # When retrieval was successful
        if buckets_status == 200:

            # When owner has buckets
            if len(buckets_list) > 0:
                return {'message': 'pong all!', 'data': buckets_list}

            resp.status_code = status.HTTP_206_PARTIAL_CONTENT
            return {'message': 'No buckets were found'}

    check, check_status, check_message = await check_bucket(bucket_name, s3_session)

    # When response is not successful
    if not check:
        resp.status_code = check_status
        return {'message': check_message}

    return {"message": "pong!"}


@app.get("/get-contents/{bucket_name}")
async def get_bucket_contents(bucket_name: str, region_name: str, resp: Response):
    # Create session
    s3_session = await aws_s3_session(region_name)
    # Check if bucket exists
    check, check_status, check_message = await check_bucket(bucket_name, s3_session)  # 200|206|404|400

    if not check:
        resp.status_code = check_status
        return {'message': check_message}

    # When bucket exists
    if check:
        contents = await bucket_contents(bucket_name, s3_session)
        return {"message": 'Contents successfully retrieved!', "data": contents}


@app.get("/get-file-details/{bucket_name}/{file_id}")
async def get_file_content_contents(bucket_name: str, region_name: str, file_id: str, app_name: str):
    s3_session = await aws_s3_session(region_name)
    file_name = await get_file_name_by_id(file_id, app_name)

    # When file name has been retrieved
    if file_name:
        # Check if bucket and file exist
        check = await check_bucket_file(
            bucket_name, file_name, s3_session
        )

        # When file has been identified in bucket
        if check:
            return {"message": 'File details successfully retrieved!', 'data': file_name}


@app.get("/download/{bucket_name}/{file_id}")
async def download_file(bucket_name: str, file_id: str, region_name: str, app_name: str):
    s3_session = await aws_s3_session(region_name)
    file_name = await get_file_name_by_id(file_id, app_name)
    check, check_status, check_message = await check_bucket(bucket_name, s3_session)

    # When bucket exists
    if check:
        contents_data: list[dict] = await bucket_contents(bucket_name, s3_session)
        contents_list = [content.get('name') for content in contents_data if content.get('type') == 'file']

        # When file is in the bucket
        if file_name in contents_list:
            local_file_path, local_file_name, file_extension = await download_from_bucket(
                bucket_name, file_name, s3_session
            )
            media_type = get_file_media_type(file_extension)
            return FileResponse(local_file_path, filename=local_file_name, media_type=media_type)


@app.post('/new')
async def add_new_bucket(bucket: Bucket, resp: Response):
    # Create session
    s3_session = await aws_s3_session(bucket.region_name)
    # Check if bucket exists
    check, check_status, check_message = await check_bucket(bucket.bucket_name, s3_session)  # 200|206|404|400

    # When returned status is not 'not existing'
    if check_status != 404:

        # When bucket already exists
        if check_status == 200:
            resp.status_code = status.HTTP_206_PARTIAL_CONTENT
            return {'message': 'Bucket already exists'}

        resp.status_code = check_status
        return {'message': check_message}

    # When bucket does not exist
    if check_status == 404:
        try:
            creation_response = s3_session.create_bucket(
                Bucket=bucket.bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': bucket.region_name,
                }
            )
            response_status = await get_response_status(creation_response)

            # When bucket creation is successful
            if response_status == 200:
                resp.status_code = status.HTTP_201_CREATED
                return {'message': 'Bucket successfully created!'}

        except ClientError:
            check_message = 'Check region settings'
            info(check_message)
            resp.status_code = status.HTTP_400_BAD_REQUEST
            return {'message': check_message}


@app.post('/upload', status_code=status.HTTP_201_CREATED)
async def upload_file(bucket_name: Annotated[str, Form()], region_name: Annotated[str, Form()],
                      file_name: Annotated[str, Form()], app_name: Annotated[str, Form()],
                      file: Annotated[UploadFile, File()], resp: Response):
    # Create session
    s3_session = await aws_s3_session(region_name)

    # Check if bucket exists
    _check_bucket_file, _check_bucket_file_status, _check_bucket_file_message = await check_bucket_file(
        bucket_name, file_name, s3_session
    )

    if _check_bucket_file_status != 404:
        resp.status_code = _check_bucket_file_status
        return {'message': _check_bucket_file_message}

    # When file does not exist
    if _check_bucket_file_status == 404:
        # Prepare file name
        formatted_file_name, file_extension = prepare_file_name(file_name)

        # Save file to directory
        app_dir_name = 'app_dir_{}'.format(app_name)
        makedirs(app_dir_name, exist_ok=True)
        temporary_file_name = generate_temporary_filename(file_extension)
        file_path = "{}/{}".format(app_dir_name, temporary_file_name)
        with open(file_path, "wb") as f:
            # Read the entire file content
            file_content = await file.read()

            # Write the file content to the destination
            f.write(file_content)

        f.close()

        upload_record_id, upload_response_status = await upload_to_bucket(
            bucket_name, file_path, file_name, app_name, s3_session
        )

        # When upload was successful
        if upload_response_status == 200:
            return {'message': 'Upload successful', 'data': upload_record_id}


@app.delete('/delete/{bucket_name}')
async def delete_bucket(bucket_name: str, region_name: str, resp: Response):
    # Create session
    s3_session = await aws_s3_session(region_name)
    # Check if bucket exists
    check, check_status, check_message = await check_bucket(bucket_name, s3_session)  # 200|206|404|400

    if not check:
        resp.status_code = check_status
        return {'message': check_message}

    if check:
        session_response = s3_session.delete_bucket(Bucket=bucket_name)
        response_status = await get_response_status(session_response)

        if response_status == 204:
            resp.status_code = status.HTTP_200_OK
            return {'message': 'Bucket successfully deleted!'}
