package com.queuedrop.user.application.service.fake;

import com.queuedrop.user.domain.exception.DuplicateEmailException;
import com.queuedrop.user.domain.model.Email;
import com.queuedrop.user.domain.model.User;
import com.queuedrop.user.domain.repository.UserRepository;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** 이메일 유니크를 흉내 내는 메모리 저장소. 저장 시 id 를 부여한다. */
public class InMemoryUserRepository implements UserRepository {

    private final Map<Email, User> store = new LinkedHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private boolean throwDuplicateOnSave;

    /** exists 확인은 통과했지만 저장 시점에 다른 요청이 먼저 저장한 상황(동시 가입)을 재현한다. */
    public void throwDuplicateOnSave() {
        this.throwDuplicateOnSave = true;
    }

    @Override
    public User save(User user) {
        if (throwDuplicateOnSave || store.containsKey(user.getEmail())) {
            throw new DuplicateEmailException(user.getEmail().value());
        }
        User saved = User.reconstitute(sequence.incrementAndGet(), user.getEmail(), user.getPasswordHash(),
                user.getNickname(), user.getRole(), user.getCreatedAt());
        store.put(saved.getEmail(), saved);
        return saved;
    }

    @Override
    public Optional<User> findByEmail(Email email) {
        return Optional.ofNullable(store.get(email));
    }

    @Override
    public boolean existsByEmail(Email email) {
        return store.containsKey(email);
    }

    /** 컨텍스트에 싱글턴으로 올린 경우 테스트 간 상태를 비운다. */
    public void clear() {
        store.clear();
        sequence.set(0);
        throwDuplicateOnSave = false;
    }

    public List<User> findAll() {
        return new ArrayList<>(store.values());
    }
}
