FROM python:3.12

# Set the working directory
WORKDIR /app

# install reqs
RUN apt-get update && apt-get install -y supervisor

# Copy the files into the container
COPY requirements.txt /app/
COPY lib/snowflake_ingest_streaming-0.2.0+rel.test.97.g0311ff1-cp39-abi3-macosx_11_0_arm64.whl /app/lib/snowflake_ingest_streaming-0.2.0+rel.test.97.g0311ff1-cp39-abi3-macosx_11_0_arm64.whl
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY .env /app/
COPY *.py /app/
COPY models/ /app/models/
COPY storage/ /app/storage/
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["/usr/bin/supervisord"]