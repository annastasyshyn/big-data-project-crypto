FROM bitnami/spark:3.5
USER root
COPY requirements.txt /opt/app/
RUN pip install --no-cache-dir -r /opt/app/requirements.txt
COPY *.py /opt/bitnami/spark/jobs/
USER 1001
