package com.queuedrop.ranking.domain.model;

/** rank 는 1부터 시작하는 순번(동점이어도 순번은 다름). memberId 는 보드에 따라 dropId 또는 userId. */
public record RankEntry(long rank, long memberId, long score) {}
