FROM bitnami/spark:3.4

COPY requirements.txt .

RUN pip3 install -r requirements.txt