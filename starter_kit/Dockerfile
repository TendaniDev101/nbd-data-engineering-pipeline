FROM nedbank-de-challenge/base:1.0

# Ensure pyspark launcher resolves correctly even if base image path conventions vary.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ pipeline/
COPY config/ config/

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "pipeline/run_all.py"]
