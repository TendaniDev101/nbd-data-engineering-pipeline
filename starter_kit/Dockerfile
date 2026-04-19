FROM nedbank-de-challenge/base:1.0

# Ensure pyspark launcher resolves correctly even if base image path conventions vary.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV SPARK_LOCAL_IP=127.0.0.1
ENV DELTA_JARS=/opt/delta-jars/delta-spark_2.12-3.1.0.jar,/opt/delta-jars/delta-storage-3.1.0.jar
ENV JAVA_TOOL_OPTIONS=-Dorg.xerial.snappy.tempdir=/data/output/.snappy

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bundle Delta jars in the image so Spark can run fully offline (--network=none).
RUN mkdir -p /opt/delta-jars \
    && curl -fsSL -o /opt/delta-jars/delta-spark_2.12-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
    && curl -fsSL -o /opt/delta-jars/delta-storage-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ pipeline/
COPY config/ config/

# Entry point — run as a package module so absolute imports resolve correctly.
CMD ["python", "-m", "pipeline.run_all"]
