package com.queuedrop.ranking.application.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/** 대상 클래스 로거에 ListAppender 를 붙여 로그를 수집한다 (coupon LogCapture 와 같은 방식). */
final class LogCapture implements AutoCloseable {

    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    LogCapture(Class<?> target) {
        this.logger = (Logger) LoggerFactory.getLogger(target);
        appender.start();
        logger.addAppender(appender);
    }

    List<ILoggingEvent> events(Level level) {
        return appender.list.stream().filter(e -> e.getLevel() == level).toList();
    }

    List<String> messages(Level level) {
        return events(level).stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    void clear() {
        appender.list.clear();
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        appender.stop();
    }
}
