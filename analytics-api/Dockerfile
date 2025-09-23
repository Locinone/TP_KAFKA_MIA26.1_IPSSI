FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV WEBHDFS_BASE=http://namenode:9870/webhdfs/v1
ENV WEATHER_DATA_PATH=/weather-data
ENV ALERTS_PATH=/hdfs-data

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
