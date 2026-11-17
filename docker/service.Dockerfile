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

# 힙 50%: 75% 면 G1 이 커밋한 힙(576m) + 비힙(약 160m) + 스레드·네이티브가 768m 한도를 넘어 OOM kill (phase-8 #8 P4).
# 측정한 힙 사용 최대는 약 180m 라 384m 로 충분하다. gateway 는 compose 에서 다이렉트 메모리까지 따로 지정
ENV JAVA_OPTS="-XX:MaxRAMPercentage=50 -XX:+UseG1GC"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
