FROM apache/spark:3.5.3-python3

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/app/src
CMD ["python", "-m", "spark_pipeline.run_pipeline", "--config", "config/pipeline.yaml"]
