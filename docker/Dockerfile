FROM python:3.12

# Set the working directory
WORKDIR /app

# install reqs
RUN apt-get update && apt-get install -y supervisor

# Copy the files into the container
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY .env /app/
COPY *.py /app/
COPY models/ /app/models/
COPY storage/ /app/storage/
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["/usr/bin/supervisord"]