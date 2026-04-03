FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app

# Ensure we always run the newly built fat jar
COPY target/*-SNAPSHOT.jar app.jar

CMD ["java", "-jar", "app.jar"]
