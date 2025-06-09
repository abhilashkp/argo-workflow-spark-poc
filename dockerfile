FROM spark:3.5.0

# Swith to root user to install packages and create directories
USER root:root

RUN mkdir -p /app
RUN mkdir -p /opt/spark/conf
RUN mkdir -p /opt/spark/jars
RUN mkdir -p /opt/spark/pyscripts
RUN mkdir -p /opt/spark/pypackages
RUN mkdir -p /opt/spark/external_libs

# Will be stored in /opt/spark/pyscripts/src
COPY ./packages/ifsutils.tgz /opt/spark/pypackages
COPY ./external_libs/pycryptodome-3.20.0.tgz /opt/spark/external_libs


COPY ./jars/hadoop-azure-3.4.0.jar /opt/spark/jars
COPY ./jars/hadoop-common-3.4.0.jar /opt/spark/jars
COPY ./jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar /opt/spark/jars
COPY ./jars/azurebfs-sas-token-provider-3.4.0.jar /opt/spark/jars
COPY ./jars/spark-excel_2.12-3.5.1_0.20.4.jar /opt/spark/jars

COPY ./log4j2.properties $SPARK_HOME/log4j2-conf/

# Check if PYTHONPATH is set and incorporate it into the new PYTHONPATH
# RUN if [ -z "$PYTHONPATH" ]; then \
#         PYTHONPATH="/opt/spark/pypackages"; \
#     else \
#         PYTHONPATH="/opt/spark/pypackages:${PYTHONPATH}"; \
#     fi && \
#     echo "PYTHONPATH=$PYTHONPATH" >> /etc/environment

ENV PYTHONPATH="/opt/spark/pypackages:${PYTHONPATH}"

COPY ./jars/jmx_prometheus_javaagent-0.16.1.jar /opt/spark/jars
RUN mkdir -p /opt/spark/metric-conf
RUN chmod -R 777 /opt/spark/metric-conf
COPY jmx_prometheus_javaagent.yaml /opt/spark/metric-conf/


# this folder will contain the actual solution scripts, here its only created.
RUN mkdir -p /opt/spark/pyscripts/src

#Extract all tar files in /opt/spark/pypackages and remove the tar files, this folder contains the ifs shared scripts
RUN for file in /opt/spark/pypackages/*.tgz; do tar -xvzf "$file" -C /opt/spark/pypackages && rm "$file"; done
#Extract packages that need to be compiled and remove the tar files, this folder contains external shared libraries
RUN for file in /opt/spark/external_libs/*.tgz; do tar -xvzf "$file" -C /opt/spark/external_libs && rm "$file"; done

#Run setup.py for these packages from the directory they are extracted to
#Note: By default these packages will be installed to /usr/local/lib/python3.8/dist-packages
RUN cd /opt/spark/external_libs/pycryptodome-3.20.0 && python3 setup.py install

# ensure that the sparkuser can write the shuffle file and the logs to mounted volumes
#RUN chown spark:spark -R /mnt

# set the user back to sparkuser, with limited permissions
USER spark:spark
ADD write_jobhistory_to_adls.py /opt/spark/pyscripts/src/write_jobhistory_to_adls.py
ADD iceberg_compaction_test.py /opt/spark/pyscripts/src/iceberg_compaction_test.py
ADD create_iceberg_table_from_json.py /opt/spark/pyscripts/src/create_iceberg_table_from_json.py
ADD create_iceberg_table-polaris.py /opt/spark/pyscripts/src/create_iceberg_table-polaris.py

WORKDIR /app

ENTRYPOINT [ "/opt/entrypoint.sh" ]