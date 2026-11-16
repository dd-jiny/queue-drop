package com.queuedrop.product.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.product.application.dto.DropRegisterCommand;
import com.queuedrop.product.application.dto.DropResult;
import com.queuedrop.product.application.dto.Requester;
import com.queuedrop.product.application.exception.ProductErrorCode;
import com.queuedrop.product.application.port.DropCreatedPayload;
import com.queuedrop.product.application.service.fake.FakeDropEventPublisher;
import com.queuedrop.product.application.service.fake.FakeDropRepository;
import com.queuedrop.product.application.service.fake.FakeProductRepository;
import com.queuedrop.product.domain.exception.DomainException;
import com.queuedrop.product.domain.model.Drop;
import com.queuedrop.product.domain.model.Product;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DropServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-16T09:00:00Z");
    private static final Instant OPEN_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant CLOSE_AT = Instant.parse("2026-11-16T12:00:00Z");
    private static final String TITLE = "한정판 스니커즈 드로우";
    private static final Requester ADMIN = new Requester(1L, "ADMIN");

    private FakeProductRepository productRepository;
    private FakeDropRepository dropRepository;
    private FakeDropEventPublisher eventPublisher;
    private DropService dropService;
    private Long productId;

    @BeforeEach
    void setUp() {
        productRepository = new FakeProductRepository();
        dropRepository = new FakeDropRepository();
        eventPublisher = new FakeDropEventPublisher();
        dropService = new DropService(productRepository, dropRepository, eventPublisher,
                Clock.fixed(NOW, ZoneOffset.UTC));
        // 서비스를 거치지 않고 저장소에 직접 상품을 둔다
        productId = productRepository.save(Product.create("한정판 스니커즈", null, null, NOW)).getId();
    }

    private DropRegisterCommand command(Requester requester, Long productId, int entryCapacity, int winnerCount) {
        return new DropRegisterCommand(requester, productId, TITLE, entryCapacity, winnerCount, OPEN_AT, CLOSE_AT);
    }

    /** 서비스를 거치지 않고 저장소에 직접 드로우를 둔다. */
    private Drop givenSavedDrop() {
        return dropRepository.save(Drop.create(productId, TITLE, 100, 10, OPEN_AT, CLOSE_AT, NOW));
    }

    static Stream<Arguments> nonAdminRequesters() {
        return Stream.of(
                Arguments.of("USER", new Requester(2L, "USER")),
                Arguments.of("role null", new Requester(2L, null)),
                Arguments.of("소문자 admin", new Requester(2L, "admin")),
                Arguments.of("requester null", null));
    }

    @Test
    @DisplayName("[T2-11] ADMIN 이 존재하는 상품으로 등록하면 저장되고 payload 1건이 필드 일치로 발행되며 결과 status 는 SCHEDULED")
    void adminRegistersDrop() {
        // Given
        DropRegisterCommand command = command(ADMIN, productId, 100, 10);

        // When
        DropResult result = dropService.register(command);

        // Then: 저장 상태
        assertThat(dropRepository.findAll()).hasSize(1);
        Drop saved = dropRepository.findAll().getFirst();
        assertThat(saved.getProductId()).isEqualTo(productId);
        assertThat(saved.getTitle()).isEqualTo(TITLE);
        assertThat(saved.getEntryCapacity()).isEqualTo(100);
        assertThat(saved.getWinnerCount()).isEqualTo(10);
        assertThat(saved.getOpenAt()).isEqualTo(OPEN_AT);
        assertThat(saved.getCloseAt()).isEqualTo(CLOSE_AT);
        assertThat(saved.isDrawn()).isFalse();
        assertThat(saved.getCreatedAt()).isEqualTo(NOW);

        // Then: 결과
        assertThat(result.id()).isNotNull().isEqualTo(saved.getId());
        assertThat(result.productId()).isEqualTo(productId);
        assertThat(result.title()).isEqualTo(TITLE);
        assertThat(result.entryCapacity()).isEqualTo(100);
        assertThat(result.winnerCount()).isEqualTo(10);
        assertThat(result.openAt()).isEqualTo(OPEN_AT);
        assertThat(result.closeAt()).isEqualTo(CLOSE_AT);
        assertThat(result.status()).isEqualTo("SCHEDULED");
        assertThat(result.createdAt()).isEqualTo(NOW);

        // Then: 발행 (저장 후 부여된 id 가 담긴다)
        assertThat(eventPublisher.created()).containsExactly(new DropCreatedPayload(
                saved.getId(), productId, TITLE, 100, 10, OPEN_AT, CLOSE_AT));
    }

    @ParameterizedTest(name = "[T2-12] register - {0}")
    @MethodSource("nonAdminRequesters")
    @DisplayName("[T2-12] ADMIN 이 아니면 register 는 COMMON_FORBIDDEN, 저장·발행 없음")
    void nonAdminCannotRegister(String label, Requester requester) {
        // Given
        DropRegisterCommand command = command(requester, productId, 100, 10);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> dropService.register(command))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CommonErrorCode.FORBIDDEN));
        assertThat(dropRepository.findAll()).isEmpty();
        assertThat(eventPublisher.created()).isEmpty();
    }

    @ParameterizedTest(name = "[T2-12] republish - {0}")
    @MethodSource("nonAdminRequesters")
    @DisplayName("[T2-12] ADMIN 이 아니면 republish 는 COMMON_FORBIDDEN, 발행 없음")
    void nonAdminCannotRepublish(String label, Requester requester) {
        // Given
        Drop saved = givenSavedDrop();

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> dropService.republish(requester, saved.getId()))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CommonErrorCode.FORBIDDEN));
        assertThat(eventPublisher.created()).isEmpty();
    }

    @Test
    @DisplayName("[T2-12] 권한 검사가 상품 존재 확인보다 먼저다 (USER + 없는 상품 → COMMON_FORBIDDEN)")
    void authorizationBeforeProductLookup() {
        // Given
        DropRegisterCommand command = command(new Requester(2L, "USER"), 999L, 100, 10);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> dropService.register(command))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(CommonErrorCode.FORBIDDEN));
    }

    @Test
    @DisplayName("[T2-13] 없는 productId 로 등록하면 PRODUCT_NOT_FOUND, 저장·발행 없음")
    void unknownProduct() {
        // Given
        DropRegisterCommand command = command(ADMIN, 999L, 100, 10);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> dropService.register(command))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(ProductErrorCode.PRODUCT_NOT_FOUND));
        assertThat(dropRepository.findAll()).isEmpty();
        assertThat(eventPublisher.created()).isEmpty();
    }

    @Test
    @DisplayName("[T2-14] M<K 이면 PRODUCT_INVALID_DROP, cause 는 DomainException(상세 메시지 보존), 저장·발행 없음")
    void invalidDropWrapsDomainException() {
        // Given
        DropRegisterCommand command = command(ADMIN, productId, 10, 11);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> dropService.register(command))
                .satisfies(e -> {
                    assertThat(e.errorCode()).isEqualTo(ProductErrorCode.INVALID_DROP);
                    assertThat(e.getCause()).isInstanceOf(DomainException.class);
                    assertThat(e.getCause().getMessage()).isNotBlank();
                    // 응답 메시지는 코드별 고정 문구 (도메인 상세는 cause 에만)
                    assertThat(e.getMessage()).isEqualTo(ProductErrorCode.INVALID_DROP.message());
                });
        assertThat(dropRepository.findAll()).isEmpty();
        assertThat(eventPublisher.created()).isEmpty();
    }

    @Test
    @DisplayName("[T2-14] 오픈 1분 여유 미달도 PRODUCT_INVALID_DROP (now 는 주입된 Clock 기준)")
    void openWithinLeadTimeIsInvalidDrop() {
        // Given: NOW=09:00, openAt=09:00:59
        DropRegisterCommand command = new DropRegisterCommand(ADMIN, productId, TITLE, 100, 10,
                Instant.parse("2026-11-16T09:00:59Z"), CLOSE_AT);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> dropService.register(command))
                .satisfies(e -> {
                    assertThat(e.errorCode()).isEqualTo(ProductErrorCode.INVALID_DROP);
                    assertThat(e.getCause()).isInstanceOf(DomainException.class);
                });
        assertThat(dropRepository.findAll()).isEmpty();
        assertThat(eventPublisher.created()).isEmpty();
    }

    @Test
    @DisplayName("[T2-15] 저장된 드로우를 republish 하면 저장 값 그대로 payload 1건 발행")
    void republishSavedDrop() {
        // Given
        Drop saved = givenSavedDrop();

        // When
        dropService.republish(ADMIN, saved.getId());

        // Then
        assertThat(eventPublisher.created()).containsExactly(new DropCreatedPayload(
                saved.getId(), productId, TITLE, 100, 10, OPEN_AT, CLOSE_AT));
    }

    @Test
    @DisplayName("[T2-15] 없는 dropId 를 republish 하면 PRODUCT_DROP_NOT_FOUND, 발행 없음")
    void republishUnknownDrop() {
        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> dropService.republish(ADMIN, 999L))
                .satisfies(e -> assertThat(e.errorCode()).isEqualTo(ProductErrorCode.DROP_NOT_FOUND));
        assertThat(eventPublisher.created()).isEmpty();
    }

    @Test
    @DisplayName("[T2-15] 이미 오픈된 드로우도 republish 할 수 있다 (수신 측 멱등)")
    void republishOpenDrop() {
        // Given: 오픈 시각이 지난 드로우 (restore 로 직접 저장)
        Drop open = dropRepository.save(Drop.restore(null, productId, TITLE, 100, 10,
                Instant.parse("2026-11-16T08:00:00Z"), CLOSE_AT, false, Instant.parse("2026-11-15T00:00:00Z")));

        // When
        dropService.republish(ADMIN, open.getId());

        // Then
        assertThat(eventPublisher.created()).hasSize(1);
        assertThat(eventPublisher.created().getFirst().dropId()).isEqualTo(open.getId());
    }

    @Test
    @DisplayName("[T2-15] republish 를 두 번 호출하면 같은 payload 가 두 번 발행된다 (멱등은 수신 측 책임, 저장 상태는 그대로)")
    void republishTwice() {
        // Given
        Drop saved = givenSavedDrop();

        // When
        dropService.republish(ADMIN, saved.getId());
        dropService.republish(ADMIN, saved.getId());

        // Then
        DropCreatedPayload expected = new DropCreatedPayload(saved.getId(), productId, TITLE, 100, 10, OPEN_AT, CLOSE_AT);
        assertThat(eventPublisher.created()).containsExactly(expected, expected);
        assertThat(dropRepository.findAll()).hasSize(1);
    }

    @Test
    @DisplayName("[T2-11] 오픈 여유가 정확히 1분이어도 등록되고, 결과 status 는 주입된 Clock(now) 기준 SCHEDULED 다")
    void registerResultStatusUsesNow() {
        // Given: NOW=09:00, openAt=09:01:00
        Instant openAt = Instant.parse("2026-11-16T09:01:00Z");
        DropRegisterCommand command = new DropRegisterCommand(ADMIN, productId, TITLE, 10, 10, openAt, CLOSE_AT);

        // When
        DropResult result = dropService.register(command);

        // Then
        assertThat(result.status()).isEqualTo("SCHEDULED");
        assertThat(result.createdAt()).isEqualTo(NOW);
        assertThat(eventPublisher.created()).hasSize(1);
    }

    static Stream<Arguments> limitViolations() {
        return Stream.of(
                Arguments.of("openAt 이 now+365일 초과", TITLE,
                        NOW.plus(java.time.Duration.ofDays(366)), NOW.plus(java.time.Duration.ofDays(366)).plusSeconds(3600)),
                Arguments.of("응모 기간 30일 초과", TITLE, OPEN_AT, OPEN_AT.plus(java.time.Duration.ofDays(31))),
                Arguments.of("제목 101자", "가".repeat(101), OPEN_AT, CLOSE_AT));
    }

    @ParameterizedTest(name = "[T2-14] {0}")
    @MethodSource("limitViolations")
    @DisplayName("[T2-14] 기간·길이 규칙 위반은 PRODUCT_INVALID_DROP, cause 는 DomainException, 저장·발행 없음")
    void limitViolationsAreInvalidDrop(String label, String title, Instant openAt, Instant closeAt) {
        // Given
        DropRegisterCommand command = new DropRegisterCommand(ADMIN, productId, title, 100, 10, openAt, closeAt);

        // When / Then
        assertThatExceptionOfType(ApplicationException.class)
                .isThrownBy(() -> dropService.register(command))
                .satisfies(e -> {
                    assertThat(e.errorCode()).isEqualTo(ProductErrorCode.INVALID_DROP);
                    assertThat(e.getCause()).isInstanceOf(DomainException.class);
                });
        assertThat(dropRepository.findAll()).isEmpty();
        assertThat(eventPublisher.created()).isEmpty();
    }

    @Test
    @DisplayName("[T2-11] 나노초가 섞인 시각으로 등록하면 저장 값·결과·발행 payload 모두 마이크로초로 내림된 같은 값이다")
    void nanosTruncatedConsistently() {
        // Given
        Instant openAt = Instant.parse("2026-11-16T10:00:00.123456789Z");
        Instant closeAt = Instant.parse("2026-11-16T12:00:00.987654321Z");
        Instant openMicros = Instant.parse("2026-11-16T10:00:00.123456Z");
        Instant closeMicros = Instant.parse("2026-11-16T12:00:00.987654Z");

        // When
        DropResult result = dropService.register(new DropRegisterCommand(ADMIN, productId, TITLE, 100, 10, openAt, closeAt));

        // Then
        Drop saved = dropRepository.findAll().getFirst();
        assertThat(saved.getOpenAt()).isEqualTo(openMicros);
        assertThat(saved.getCloseAt()).isEqualTo(closeMicros);
        assertThat(result.openAt()).isEqualTo(openMicros);
        assertThat(result.closeAt()).isEqualTo(closeMicros);
        assertThat(eventPublisher.created().getFirst().openAt()).isEqualTo(openMicros);
        assertThat(eventPublisher.created().getFirst().closeAt()).isEqualTo(closeMicros);
    }
}
