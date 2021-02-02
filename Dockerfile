FROM jupyter/pyspark-notebook

WORKDIR /app

COPY . .

RUN pip3 install -r sparkms/requirements.txt
RUN pip3 install -r tests/requirements.txt