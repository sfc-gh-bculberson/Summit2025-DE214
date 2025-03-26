FROM python:latest

# Set the working directory
WORKDIR /app

# Install any needed packages specified in requirements.txt
COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt
COPY snowflake_ingest-0.0.1.tar.gz /app/snowflake_ingest-0.0.1.tar.gz
RUN tar -xzf snowflake_ingest-0.0.1.tar.gz
RUN pip install ./snowflake_ingest-0.0.1

# Copy the files into the container at /app
COPY .env /app
COPY app.py /app
COPY generator.py /app

CMD ["python", "app.py"]