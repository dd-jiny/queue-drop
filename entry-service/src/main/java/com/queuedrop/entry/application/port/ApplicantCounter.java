package com.queuedrop.entry.application.port;

/** Redis 응모자 수 조회 (SCARD entry:{dropId}:applicants). Redis 장애 시 GateUnavailableException */
public interface ApplicantCounter {

    long count(long dropId);
}
