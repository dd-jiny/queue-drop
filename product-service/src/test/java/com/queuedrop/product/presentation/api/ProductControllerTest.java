package com.queuedrop.product.presentation.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.product.application.dto.ProductCreateCommand;
import com.queuedrop.product.application.dto.ProductResult;
import com.queuedrop.product.application.dto.Requester;
import com.queuedrop.product.application.exception.ProductErrorCode;
import com.queuedrop.product.application.service.DropQueryService;
import com.queuedrop.product.application.service.DropService;
import com.queuedrop.product.application.service.ProductService;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/** presentation 슬라이스. domain 타입은 import 하지 않는다 (application Command/Result 만 사용). */
@WebMvcTest({DropController.class, ProductController.class})
@Import({GlobalExceptionHandler.class, RequesterResolver.class})
class ProductControllerTest {

    private static final Instant CREATED_AT = Instant.parse("2026-11-16T10:00:00Z");
    private static final String VALID_BODY = """
            {"name":"한정판 스니커즈","description":"설명","imageUrl":"https://img.example.com/a.png"}""";

    @Autowired MockMvc mockMvc;
    @MockitoBean ProductService productService;
    @MockitoBean DropService dropService;
    @MockitoBean DropQueryService dropQueryService;

    private static MockHttpServletRequestBuilder createRequest(String body) {
        return post("/api/products").contentType(MediaType.APPLICATION_JSON).content(body);
    }

    private ResultActions createAs(String userId, String role, String body) throws Exception {
        return mockMvc.perform(createRequest(body).header("X-User-Id", userId).header("X-User-Role", role));
    }

    @Test
    @DisplayName("[T2-27] ADMIN 이 상품을 등록하면 201 과 ProductResponse, 헤더 값이 Requester 로 그대로 전달된다")
    void adminCreatesProduct() throws Exception {
        // Given
        given(productService.create(any())).willReturn(new ProductResult(
                5L, "한정판 스니커즈", "설명", "https://img.example.com/a.png", CREATED_AT));

        // When
        ResultActions result = createAs("7", "ADMIN", VALID_BODY);

        // Then: 응답
        result.andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(5))
                .andExpect(jsonPath("$.name").value("한정판 스니커즈"))
                .andExpect(jsonPath("$.description").value("설명"))
                .andExpect(jsonPath("$.imageUrl").value("https://img.example.com/a.png"))
                .andExpect(jsonPath("$.createdAt").value("2026-11-16T10:00:00Z"));

        // Then: Command
        ArgumentCaptor<ProductCreateCommand> captor = ArgumentCaptor.forClass(ProductCreateCommand.class);
        verify(productService).create(captor.capture());
        assertThat(captor.getValue()).isEqualTo(new ProductCreateCommand(
                new Requester(7L, "ADMIN"), "한정판 스니커즈", "설명", "https://img.example.com/a.png"));
    }

    @Test
    @DisplayName("[T2-27] USER 가 등록하면 서비스의 FORBIDDEN 이 403 COMMON_FORBIDDEN 과 고정 메시지로 매핑된다")
    void userIsForbidden() throws Exception {
        // Given
        given(productService.create(any())).willThrow(new ApplicationException(CommonErrorCode.FORBIDDEN));

        // When
        ResultActions result = createAs("8", "USER", VALID_BODY);

        // Then
        result.andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("COMMON_FORBIDDEN"))
                .andExpect(jsonPath("$.message").value(CommonErrorCode.FORBIDDEN.message()));
        ArgumentCaptor<ProductCreateCommand> captor = ArgumentCaptor.forClass(ProductCreateCommand.class);
        verify(productService).create(captor.capture());
        assertThat(captor.getValue().requester()).isEqualTo(new Requester(8L, "USER"));
    }

    @Test
    @DisplayName("[T2-27] X-User-Id·X-User-Role 헤더가 모두 없으면 400 COMMON_MISSING_USER_HEADER, 서비스는 호출되지 않는다")
    void missingHeaders() throws Exception {
        // When
        ResultActions result = mockMvc.perform(createRequest(VALID_BODY));

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
        verifyNoInteractions(productService);
    }

    @Test
    @DisplayName("[T2-27] X-User-Role 헤더만 없어도 400 COMMON_MISSING_USER_HEADER")
    void missingRoleHeader() throws Exception {
        // When
        ResultActions result = mockMvc.perform(createRequest(VALID_BODY).header("X-User-Id", "7"));

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_MISSING_USER_HEADER"));
        verifyNoInteractions(productService);
    }

    @Test
    @DisplayName("[T2-27] X-User-Id 가 숫자가 아니면 400 COMMON_INVALID_INPUT, 서비스는 호출되지 않는다")
    void nonNumericUserId() throws Exception {
        // When
        ResultActions result = createAs("abc", "ADMIN", VALID_BODY);

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"));
        verifyNoInteractions(productService);
    }

    @Test
    @DisplayName("[T2-27] 상품명이 공백이면 400 COMMON_INVALID_INPUT 과 name fieldError, 서비스는 호출되지 않는다")
    void blankName() throws Exception {
        // When
        ResultActions result = createAs("7", "ADMIN", """
                {"name":"   "}""");

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("COMMON_INVALID_INPUT"))
                .andExpect(jsonPath("$.fieldErrors[*].field").value(hasItem("name")));
        verifyNoInteractions(productService);
    }

    @Test
    @DisplayName("[T2-27] 상품명이 101자면 400 COMMON_INVALID_INPUT 과 name fieldError")
    void tooLongName() throws Exception {
        // When
        ResultActions result = createAs("7", "ADMIN", """
                {"name":"%s"}""".formatted("가".repeat(101)));

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors[*].field").value(hasItem("name")));
        verifyNoInteractions(productService);
    }

    @Test
    @DisplayName("[T2-27] 서비스의 INVALID_PRODUCT 는 400 PRODUCT_INVALID_PRODUCT 와 고정 메시지로 매핑되고 원인 상세는 응답에 없다")
    void invalidProductMapped() throws Exception {
        // Given
        given(productService.create(any())).willThrow(new ApplicationException(
                ProductErrorCode.INVALID_PRODUCT, new IllegalArgumentException("도메인 상세 메시지")));

        // When
        ResultActions result = createAs("7", "ADMIN", VALID_BODY);

        // Then
        result.andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("PRODUCT_INVALID_PRODUCT"))
                .andExpect(jsonPath("$.message").value(ProductErrorCode.INVALID_PRODUCT.message()));
    }
}
