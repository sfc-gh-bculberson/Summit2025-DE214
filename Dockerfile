FROM python:latest

# Set the working directory
WORKDIR /app

# install reqs
RUN apt-get update && apt-get install -y supervisor

# Install any needed packages specified in requirements.txt
COPY requirements.txt /app
COPY snowflake_ingest-0.0.1.tar.gz /app/snowflake_ingest-0.0.1.tar.gz
RUN tar -xzf snowflake_ingest-0.0.1.tar.gz
RUN pip install --no-cache-dir -r requirements.txt

# Copy the files into the container at /app
COPY .env /app
COPY streamer.py /app
COPY generator.py /app
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["/usr/bin/supervisord"]