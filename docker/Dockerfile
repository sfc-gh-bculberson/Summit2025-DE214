FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY .env /app/
COPY *.py /app/
COPY models/ /app/models/

CMD ["python", "generator.py"]
