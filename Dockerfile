FROM python:3.12-slim

RUN useradd --create-home --uid 1000 bridge && mkdir /data && chown bridge /data
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bridge.py .

USER bridge
VOLUME /data
ENV PYTHONUNBUFFERED=1 DIRIGERA_TOKEN_FILE=/data/token
ENTRYPOINT ["python", "/app/bridge.py"]
