package com.queuedrop.ranking.application.dto;

import java.util.List;

public record WeeklyRankingResult(String week, List<UserRankResult> items) {}
