# Use an official Python runtime as a parent image
FROM python:3.10-slim-buster

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Create Virtual Environment
RUN pip install --upgrade pip
RUN pip install virtualenv
RUN virtualenv venv

# Activate Virtual Environment
RUN /bin/bash -c "source venv/bin/activate"

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Run Project
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]