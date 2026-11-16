package com.queuedrop.common.test.archunit.sample.bad.r3.topics.application.port;

import com.queuedrop.common.event.Topics;

public interface TopicAwarePort {
    default String topic() { return Topics.class.getName(); }
}
