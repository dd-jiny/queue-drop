package com.queuedrop.user.domain.repository;

import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.User;
import java.util.Optional;

public interface UserRepository {
    /** @throws com.queuedrop.user.domain.exception.DuplicateEmailException 이메일 유니크 위반 */
    User save(User user);
    Optional<User> findByEmail(Email email);
    boolean existsByEmail(Email email);
}
