package com.queuedrop.ranking.support;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * 로거 이름(패키지 포함) 아래 모든 로그를 수집한다. 리스너 컨테이너 스레드 등 다른 스레드의 로그도 모인다.
 * 로그를 남기는 클래스를 테스트가 알 필요가 없도록 패키지 로거("com.queuedrop.ranking")에 붙인다.
 */
public final class CapturedLogs implements AutoCloseable {

    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    public CapturedLogs(String loggerName) {
        this.logger = (Logger) LoggerFactory.getLogger(loggerName);
        appender.start();
        logger.addAppender(appender);
    }

    /** ListAppender.list 는 동기화되지 않은 ArrayList 라 appender 잠금으로 복사한다 (AppenderBase.doAppend 가 같은 잠금) */
    public List<String> messages(Level level) {
        synchronized (appender) {
            return appender.list.stream()
                    .filter(e -> e.getLevel() == level)
                    .map(ILoggingEvent::getFormattedMessage)
                    .toList();
        }
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        appender.stop();
    }
}
