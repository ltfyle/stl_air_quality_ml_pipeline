FROM python:3.11-slim

WORKDIR /app

COPY ./scripts/pipeline_historic.py /app
COPY ./scripts/AQS_tools.py /app
COPY ./scripts/historic_weather.py /app
COPY ./requirements.txt /app

VOLUME /output

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python", "pipeline_historic.py"]
