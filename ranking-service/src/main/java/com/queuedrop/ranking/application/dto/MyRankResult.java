package com.queuedrop.ranking.application.dto;

/** weeklyRank/allRank 는 랭킹에 없으면 null */
public record MyRankResult(String week, Long weeklyRank, long weeklyScore, Long allRank, long allScore) {}
