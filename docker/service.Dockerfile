# 모든 서비스 공용. 사용: docker build -f docker/service.Dockerfile --build-arg MODULE=entry-service .
# 호스트에서 ./gradlew bootJar 로 만든 실행 jar 만 복사한다 (phase-8 3.1)
FROM eclipse-temurin:21-jre

ARG MODULE
# healthcheck 용 curl (베이스 이미지 포함 여부에 의존하지 않음)
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 app

WORKDIR /app
COPY ${MODULE}/build/libs/*.jar app.jar
USER app

ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
