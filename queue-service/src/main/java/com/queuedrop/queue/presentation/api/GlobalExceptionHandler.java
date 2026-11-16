package com.queuedrop.queue.presentation.api;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.common.error.ErrorCode;
import com.queuedrop.common.error.ErrorResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.List;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /** 서비스 예외는 모두 ApplicationException 을 상속한다. 응답 message 는 ErrorCode.message() 고정. */
    @ExceptionHandler(ApplicationException.class)
    public ResponseEntity<ErrorResponse> handleApplication(ApplicationException e) {
        ErrorCode code = e.errorCode();
        if (code.status() >= 500) {
            log.error("application error: {}", code.code(), e);
        } else {
            // 도메인 예외 등 원인의 상세 메시지는 응답에 싣지 않고 여기서만 남긴다 (layered-architecture.md 4.5)
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.warn("application error: {} - {}", code.code(), cause.getMessage());
        }
        return toResponse(code);
    }

    /** @RequestBody @Valid 검증 실패 */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException e) {
        List<ErrorResponse.FieldError> fields = e.getBindingResult().getFieldErrors().stream()
                .map(f -> new ErrorResponse.FieldError(f.getField(), f.getDefaultMessage()))
                .toList();
        return ResponseEntity.badRequest().body(ErrorResponse.of(CommonErrorCode.INVALID_INPUT, fields));
    }

    /** 컨트롤러 파라미터에 직접 붙은 제약(@RequestParam @Min 등) 위반 — Spring 7 내장 메서드 검증 */
    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<ErrorResponse> handleMethodValidation(HandlerMethodValidationException e) {
        List<ErrorResponse.FieldError> fields = e.getParameterValidationResults().stream()
                .flatMap(r -> r.getResolvableErrors().stream()
                        .map(err -> new ErrorResponse.FieldError(
                                r.getMethodParameter().getParameterName(), err.getDefaultMessage())))
                .toList();
        return ResponseEntity.badRequest().body(ErrorResponse.of(CommonErrorCode.INVALID_INPUT, fields));
    }

    /** Gateway 를 거치지 않아 X-User-* 헤더가 없는 요청 (layered-architecture.md 4.5). 그 밖의 필수 헤더 누락은 400 INVALID_INPUT */
    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingHeader(MissingRequestHeaderException e) {
        boolean userHeader = e.getHeaderName().regionMatches(true, 0, "X-User-", 0, "X-User-".length());
        return toResponse(userHeader ? CommonErrorCode.MISSING_USER_HEADER : CommonErrorCode.INVALID_INPUT);
    }

    /** 본문 파싱 실패, 경로·쿼리 타입 불일치, 필수 쿼리 누락 → 400 (catch-all 이 500 으로 바꾸지 않게) */
    @ExceptionHandler({HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class,
            MissingServletRequestParameterException.class})
    public ResponseEntity<ErrorResponse> handleBadRequest(Exception e) {
        return toResponse(CommonErrorCode.INVALID_INPUT);
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ErrorResponse> handleNoResource(NoResourceFoundException e) {
        return toResponse(CommonErrorCode.NOT_FOUND);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethodNotAllowed(HttpRequestMethodNotSupportedException e) {
        return toResponse(CommonErrorCode.METHOD_NOT_ALLOWED);
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleUnsupportedMediaType(HttpMediaTypeNotSupportedException e) {
        return toResponse(CommonErrorCode.UNSUPPORTED_MEDIA_TYPE);
    }

    /**
     * 그 밖의 예외. Spring MVC 표준 예외(org.springframework.web.ErrorResponse 구현체: ResponseStatusException,
     * ServletRequestBindingException 계열 등)가 4xx 를 뜻하면 상태별 공통 코드로 바꾸고 error 로그를 남기지 않는다.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception e) {
        if (e instanceof org.springframework.web.ErrorResponse er && er.getStatusCode().is4xxClientError()) {
            log.warn("client error: {} - {}", er.getStatusCode().value(), e.getMessage());
            return toResponse(commonCodeFor(er.getStatusCode().value()));
        }
        log.error("unexpected error", e);
        return toResponse(CommonErrorCode.INTERNAL_ERROR);
    }

    private static CommonErrorCode commonCodeFor(int status) {
        return switch (status) {
            case 401 -> CommonErrorCode.UNAUTHORIZED;
            case 403 -> CommonErrorCode.FORBIDDEN;
            case 404 -> CommonErrorCode.NOT_FOUND;
            case 405 -> CommonErrorCode.METHOD_NOT_ALLOWED;
            case 415 -> CommonErrorCode.UNSUPPORTED_MEDIA_TYPE;
            default -> CommonErrorCode.INVALID_INPUT;   // 그 밖의 4xx 는 400 으로 정규화
        };
    }

    private static ResponseEntity<ErrorResponse> toResponse(ErrorCode code) {
        return ResponseEntity.status(code.status()).body(ErrorResponse.of(code));
    }
}
