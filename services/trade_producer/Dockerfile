# specify the base image to use for Docker image
FROM python:3.11-slim-buster

# add /app/src/ to the PYTHONPATH
ENV PYTHONPATH=/app

# sets an environment variable in the container.
# PYTHONUNBUFFERED=1 ensures that Python output is
# sent directly to the console (stdout)
# without being buffered
ENV PYTHONUNBUFFERED=1

# install poetry inside the container
RUN pip install poetry

# set the working directory inside the container to /app
# All subsequent commands will be executed in this directory
WORKDIR /app

# copy the pyproject.toml and poetry.lock files into the container
COPY pyproject.toml poetry.lock /app/

# call the RUN command that uses Poetry to
# install all dependencies defined in pyproject.toml
RUN poetry install

# copy all files from local machine, `.`,  into 
# the /app directory in the container,
# this effectively transfers application code into the image
COPY . /app

# run python src/main.py using Poetry,
# execute the main.py script located in the src directory of the application
CMD ["poetry", "run", "python", "src/main.py"]