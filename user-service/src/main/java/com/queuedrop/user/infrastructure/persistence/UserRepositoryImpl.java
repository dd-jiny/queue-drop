package com.queuedrop.user.infrastructure.persistence;

import com.queuedrop.user.domain.exception.DuplicateEmailException;
import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.User;
import com.queuedrop.user.domain.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/** domain 영속성 포트 구현. DB 예외를 도메인 예외로 번역한다. */
@Repository
@RequiredArgsConstructor
class UserRepositoryImpl implements UserRepository {

    private final UserJpaRepository jpa;
    private final UserMapper mapper;

    @Override
    public User save(User user) {
        try {
            // saveAndFlush: 유니크 위반을 이 메서드 안에서 터뜨려 번역하기 위해 즉시 flush
            return mapper.toDomain(jpa.saveAndFlush(mapper.toEntity(user)));
        } catch (DataIntegrityViolationException e) {
            // users 의 무결성 제약은 email 유니크와 role CHECK 뿐이고 role 은 enum 이라 위반 불가
            throw new DuplicateEmailException(user.getEmail().value());
        }
    }

    @Override
    public Optional<User> findByEmail(Email email) {
        return jpa.findByEmail(email.value()).map(mapper::toDomain);
    }

    @Override
    public boolean existsByEmail(Email email) {
        return jpa.existsByEmail(email.value());
    }
}
