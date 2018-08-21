FROM openjdk:jre-slim
COPY build/libs/sailracetimerserver-all-1.0-SNAPSHOT.jar .
CMD java -jar sailracetimerserver-all-1.0-SNAPSHOT.jar
