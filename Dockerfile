FROM apache/airflow:2.9.2-python3.11

# Copy the requirements file from the host to the container
COPY requirements.txt /requirements.txt

# Install the Python packages
RUN pip install --no-cache-dir -r /requirements.txt
