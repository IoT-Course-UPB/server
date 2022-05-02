FROM python:3.11-rc-slim

RUN apt-get update && apt-get -y install libpq-dev gcc

COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r /usr/src/app/requirements.txt

COPY *.py /usr/src/app/

EXPOSE 6000
EXPOSE 8000

CMD ["python3", "/usr/src/app/server.py"]