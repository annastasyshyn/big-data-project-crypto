FROM apache/spark:3.5.5

USER root

COPY requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir -r /opt/app/requirements.txt

RUN mkdir -p /opt/spark/jobs/batch /tmp/.ivy2 \
 && chown -R spark:spark /opt/spark/jobs /tmp/.ivy2

COPY *.py /opt/spark/jobs/batch/

USER spark
WORKDIR /opt/spark/jobs/batch

CMD ["python3", "/opt/spark/jobs/batch/scheduler.py"]
