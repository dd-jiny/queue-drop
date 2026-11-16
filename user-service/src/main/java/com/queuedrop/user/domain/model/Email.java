package com.queuedrop.user.domain.model;

import com.queuedrop.user.domain.exception.InvalidEmailException;

import java.util.Locale;
import java.util.regex.Pattern;

/** 형식 검증·정규화를 책임지는 값 객체. 대소문자와 앞뒤 공백을 무시하고 같은 이메일로 본다. */
public record Email(String value) {

    private static final Pattern FORMAT = Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");
    private static final int MAX_LENGTH = 255;

    public Email {
        if (value == null) throw new InvalidEmailException(null);
        value = value.trim().toLowerCase(Locale.ROOT);
        if (value.length() > MAX_LENGTH || !FORMAT.matcher(value).matches()) {
            throw new InvalidEmailException(value);
        }
    }
}
