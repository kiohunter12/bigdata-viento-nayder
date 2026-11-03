FROM python:3.10-slim-trixie

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Igual a la imagen del curso + matplotlib (gráficos) y requests (API Open-Meteo)
RUN pip install --no-cache-dir pyspark==4.2.0 notebook==7.6.2 pandas==2.3.3 \
    "matplotlib>=3.8,<3.11" requests==2.32.3

WORKDIR /opt

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
