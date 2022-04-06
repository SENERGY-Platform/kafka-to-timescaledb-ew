FROM python:3-alpine
LABEL org.opencontainers.image.source https://github.com/SENERGY-Platform/kafka-to-timescaledb-ew
WORKDIR /usr/src/app
COPY . .
RUN apk --no-cache add build-base librdkafka-dev librdkafka libpq-dev git && pip install --no-cache-dir -r requirements.txt && git log -1 --pretty=format:"commit=%H%ndate=%cd%n" > git_commit && apk del git build-base librdkafka-dev libpq-dev && rm -rf .git
CMD [ "python", "-u", "main.py"]