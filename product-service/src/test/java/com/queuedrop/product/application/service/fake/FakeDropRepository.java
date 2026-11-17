package com.queuedrop.product.application.service.fake;

import com.queuedrop.product.domain.model.Drop;
import com.queuedrop.product.domain.model.DropPage;
import com.queuedrop.product.domain.model.DropStatus;
import com.queuedrop.product.domain.repository.DropRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 메모리 드로우 저장소. id 가 없으면 저장 시 부여한다.
 * search 는 도메인 규칙 Drop.status(now) 로 거르고 openAt → id 순으로 정렬한다 (JPQL 경계 일치는 T2-19 가 보장).
 * findById 는 실제 어댑터(엔티티 → Mapper 복원)처럼 매번 새 인스턴스를 돌려준다 — 조회한 객체를 바꿔도 save 없이는 저장 값이 바뀌지 않는다.
 * saveCount 는 명시 save 여부(5단계 markDrawn, E-30) 확인용이다.
 */
public class FakeDropRepository implements DropRepository {

    private final Map<Long, Drop> store = new LinkedHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private int saveCount;

    @Override
    public Drop save(Drop drop) {
        saveCount++;
        Long id = drop.getId() != null ? drop.getId() : sequence.incrementAndGet();
        if (drop.getId() != null) {
            sequence.set(Math.max(sequence.get(), id));
        }
        Drop saved = Drop.restore(id, drop.getProductId(), drop.getTitle(), drop.getEntryCapacity(),
                drop.getWinnerCount(), drop.getOpenAt(), drop.getCloseAt(), drop.isDrawn(), drop.getCreatedAt());
        store.put(id, saved);
        return saved;
    }

    @Override
    public Optional<Drop> findById(Long id) {
        return Optional.ofNullable(store.get(id)).map(FakeDropRepository::copy);
    }

    @Override
    public DropPage search(DropStatus status, Instant now, int page, int size) {
        List<Drop> filtered = store.values().stream()
                .filter(d -> status == null || d.status(now) == status)
                .sorted(Comparator.comparing(Drop::getOpenAt).thenComparing(Drop::getId))
                .toList();
        long offset = (long) page * size;   // int 곱셈 오버플로 방지
        int from = (int) Math.min(offset, filtered.size());
        int to = (int) Math.min((long) from + size, filtered.size());
        return new DropPage(filtered.subList(from, to), filtered.size(), page, size);
    }

    private static Drop copy(Drop d) {
        return Drop.restore(d.getId(), d.getProductId(), d.getTitle(), d.getEntryCapacity(), d.getWinnerCount(),
                d.getOpenAt(), d.getCloseAt(), d.isDrawn(), d.getCreatedAt());
    }

    public int saveCount() {
        return saveCount;
    }

    public List<Drop> findAll() {
        return new ArrayList<>(store.values());
    }
}
