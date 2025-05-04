FROM spark:3.5.5

USER root
# Install necessary dependencies for PySpark if not already installed
RUN pip install pyspark

USER spark

# Add your python script to the image
ADD pi.py /opt/spark/pi.py
ADD pi.py /opt/spark/json-pi.py
ADD print_params.py /app/print_params.py

# Define the entry point to run the script
CMD ["spark-submit", "/opt/spark/pi.py"]
