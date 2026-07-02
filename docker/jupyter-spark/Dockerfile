FROM monashfit/fit5202-pyspark:latest

USER root

RUN python -m pip install --no-cache-dir kafka-python \
    && python -m ipykernel install --sys-prefix --name a2-energy-pipeline --display-name "A2 Energy Pipeline"

USER 1000

WORKDIR /home/student/A2

EXPOSE 8888 4040

CMD ["bash", "-lc", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token=${JUPYTER_TOKEN:-a2-demo} --ServerApp.password=''"]
