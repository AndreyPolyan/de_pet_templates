FROM python:3.10

ARG KAFKA_HOST  
ARG KAFKA_PORT 
ARG KAFKA_DESTINATION_TOPIC

ARG REDIS_HOST
ARG REDIS_PORT

RUN apt-get update -y

COPY . .

RUN pip install -r requirements.txt

WORKDIR /src

EXPOSE 5001

ENTRYPOINT ["python"]

CMD ["app.py"]