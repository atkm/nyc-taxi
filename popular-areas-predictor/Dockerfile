FROM openjdk:8-jre
COPY ./target/popular-areas-predictor-bundled-0.1.jar /opt/
WORKDIR /opt
ENTRYPOINT ["java", "-cp", "popular-areas-predictor-bundled-0.1.jar", "xyz.akumano.kafka.prediction.PopularityPredictor"]
CMD ["--sourceTopicName", "metar", "--sinkTopicName", "prediction", "--bootstrapServer", "10.138.0.2:9092"]
