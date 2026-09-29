FROM spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root
# Register the host identity instead of running Java with an unknown numeric UID.
ARG LOCAL_UID=185
ARG LOCAL_GID=185
RUN case "$LOCAL_UID:$LOCAL_GID" in *[!0-9:]*|:*|*:) exit 1;; esac \
    && test "$LOCAL_UID" -gt 0 && test "$LOCAL_GID" -gt 0 \
    && if [ "$(id -g spark)" != "$LOCAL_GID" ]; then groupmod -o -g "$LOCAL_GID" spark; fi \
    && if [ "$(id -u spark)" != "$LOCAL_UID" ]; then usermod -u "$LOCAL_UID" spark; fi \
    && usermod -d /home/spark spark \
    && install -d -o spark -g spark /home/spark
RUN ln -s /opt/spark/python/lib/py4j-*-src.zip /opt/spark/python/lib/py4j.zip
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir --only-binary=:all: -r /tmp/requirements.txt
USER spark

WORKDIR /app
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j.zip \
    PYTHONDONTWRITEBYTECODE=1 \
    SPARK_LOCAL_IP=127.0.0.1

# PySpark and Py4J come from Spark; do not install another copy with pip.
RUN python3 -c "import sys, pyspark, py4j, duckdb; assert sys.version_info[:2] == (3, 10); assert pyspark.__version__ == '3.5.9'; assert duckdb.__version__ == '1.5.6'"
COPY --chown=spark:spark github_analytics/ /app/github_analytics/
COPY --chown=spark:spark jobs/ /app/jobs/
COPY --chown=spark:spark tests/ /app/tests/
COPY --chown=spark:spark scripts/ /app/scripts/

ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]"]
CMD ["/app/jobs/pipeline.py", "--date", "2025-06-01"]
