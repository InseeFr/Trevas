FROM jupyter/base-notebook:latest

USER root

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    openjdk-11-jre && \
    apt-get clean

COPY target/lib /vtl/lib/
COPY target/vtl-jupyter-0.5.0-SNAPSHOT.jar /vtl/vtl-jupyter.jar
COPY kernel.json /usr/local/share/jupyter/kernels/vtl/

# Set user back to priviledged user.
USER $NB_USER