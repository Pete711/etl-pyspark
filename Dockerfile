FROM python:3.11-slim

# Install system dependencies, including Java (required for PySpark)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    curl \
    && apt-get upgrade -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Download and install Spark

RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Set working directory

WORKDIR /app

# Copy requirements file and install Python dependencies

COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files from all directories

COPY app/ ./app/ 
COPY parquet-assembler-v1.0/ ./parquet-assembler-v1.0/ 
COPY parquet-slicer-v1.0/ ./parquet-slicer-v1.0/

# Copy configuration files from root directory

COPY params.json . 
COPY manifest.json .

# Command to run the ETL orchestrator (default, overridden in docker-compose.yml)

CMD ["python", "app/main.py"]