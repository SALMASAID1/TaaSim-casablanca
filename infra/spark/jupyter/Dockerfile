FROM quay.io/jupyter/pyspark-notebook:spark-3.5.0

USER root

# copy the pinned requirements into the image
COPY requirements-notebook.txt /tmp/requirements-notebook.txt

# install once at image build-time (so notebooks don't reinstall every run)
RUN pip install --no-cache-dir -r /tmp/requirements-notebook.txt \
 && fix-permissions /opt/conda \
 && fix-permissions /home/jovyan

USER jovyan