FROM spark:3.5.5

USER root

# Install necessary dependencies
RUN pip install pyspark

# Create necessary directory for your script
RUN mkdir -p /opt/spark/pyscripts/src

USER spark

# Add your Python scripts
ADD pi.py /opt/spark/pi.py
ADD pi.py /opt/spark/json-pi.py
ADD print_params.py /app/print_params.py
# ADD print_tablemetadata.py /app/print_tablemetadata.py 
ADD print_tablemetadata.py /opt/spark/pyscripts/src/print_tablemetadata.py

# Define the entry point script (optional)
# CMD ["spark-submit", "/opt/spark/pi.py"]
