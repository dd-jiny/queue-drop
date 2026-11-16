package com.queuedrop.user.infrastructure.persistence;

import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.Role;
import com.queuedrop.user.domain.model.User;
import org.springframework.stereotype.Component;

/** 도메인 모델 ↔ JPA 엔티티 변환 */
@Component
class UserMapper {

    User toDomain(UserJpaEntity e) {
        return User.reconstitute(e.getId(), new Email(e.getEmail()), e.getPasswordHash(),
                e.getNickname(), Role.valueOf(e.getRole()), e.getCreatedAt());
    }

    UserJpaEntity toEntity(User u) {
        return new UserJpaEntity(u.getId(), u.getEmail().value(), u.getPasswordHash(),
                u.getNickname(), u.getRole().name(), u.getCreatedAt());
    }
}
