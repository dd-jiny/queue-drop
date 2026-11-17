package com.queuedrop.ranking.presentation.api;

import com.queuedrop.ranking.application.service.RankingQueryService;
import com.queuedrop.ranking.presentation.api.response.DropRankingResponse;
import com.queuedrop.ranking.presentation.api.response.MyRankResponse;
import com.queuedrop.ranking.presentation.api.response.UserRankingResponse;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 랭킹 조회 API. size·week 검증은 서비스 몫이라 값을 그대로 넘긴다 (?week= 빈 문자열도 그대로 → 400 INVALID_WEEK) */
@RestController
@RequestMapping("/api/rankings")
@RequiredArgsConstructor
class RankingController {

    private final RankingQueryService queryService;

    @GetMapping("/drops")
    List<DropRankingResponse> drops(@RequestParam(defaultValue = "10") int size) {
        return queryService.topDrops(size).stream().map(DropRankingResponse::from).toList();
    }

    @GetMapping("/users/weekly")
    UserRankingResponse weekly(@RequestParam(required = false) String week,
                               @RequestParam(defaultValue = "10") int size) {
        return UserRankingResponse.from(queryService.weeklyUsers(week, size));
    }

    @GetMapping("/users/all")
    UserRankingResponse all(@RequestParam(defaultValue = "10") int size) {
        return UserRankingResponse.ofAll(queryService.allUsers(size));
    }

    /** 헤더 누락(Gateway 우회 등)은 MissingRequestHeaderException → GlobalExceptionHandler 가 400 COMMON_MISSING_USER_HEADER */
    @GetMapping("/users/me")
    MyRankResponse me(@RequestHeader("X-User-Id") Long userId) {
        return MyRankResponse.from(queryService.myRank(userId));
    }
}
