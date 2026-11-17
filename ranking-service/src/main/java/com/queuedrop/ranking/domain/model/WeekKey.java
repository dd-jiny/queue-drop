package com.queuedrop.ranking.domain.model;

import com.queuedrop.ranking.domain.exception.InvalidWeekKeyException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** ISO 주차 값 객체 (Asia/Seoul). 키 접미사·API 표기는 yyyy-Www */
public record WeekKey(int weekBasedYear, int week) {

    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    private static final Pattern FORMAT = Pattern.compile("^(\\d{4})-W(\\d{2})$");

    public WeekKey {
        if (week < 1 || week > weeksIn(weekBasedYear)) {
            throw new InvalidWeekKeyException(weekBasedYear + "-W" + week);
        }
    }

    /** 이벤트 발생 시각 기준 주차. 처리 시각(now)을 쓰지 않는다. */
    public static WeekKey of(Instant instant) {
        ZonedDateTime t = instant.atZone(ZONE);
        return new WeekKey(t.get(IsoFields.WEEK_BASED_YEAR), t.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
    }

    public static WeekKey parse(String value) {
        Matcher m = FORMAT.matcher(value);
        if (!m.matches()) {
            throw new InvalidWeekKeyException(value);
        }
        return new WeekKey(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
    }

    /** 키 접미사 및 API 표기 형식: yyyy-Www */
    public String value() {
        return String.format("%04d-W%02d", weekBasedYear, week);
    }

    private static int weeksIn(int weekBasedYear) {
        // 12/28 은 항상 그 해의 마지막 ISO 주에 속한다
        return LocalDate.of(weekBasedYear, 12, 28).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    }
}
