package com.queuedrop.product.application.service.fake;

import com.queuedrop.product.domain.model.Product;
import com.queuedrop.product.domain.repository.ProductRepository;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** 메모리 상품 저장소. id 가 없으면 저장 시 부여한다. */
public class FakeProductRepository implements ProductRepository {

    private final Map<Long, Product> store = new LinkedHashMap<>();
    private final AtomicLong sequence = new AtomicLong();

    @Override
    public Product save(Product product) {
        Long id = product.getId() != null ? product.getId() : sequence.incrementAndGet();
        Product saved = Product.restore(id, product.getName(), product.getDescription(),
                product.getImageUrl(), product.getCreatedAt());
        store.put(id, saved);
        return saved;
    }

    @Override
    public Optional<Product> findById(Long id) {
        return Optional.ofNullable(store.get(id));
    }

    @Override
    public boolean existsById(Long id) {
        return store.containsKey(id);
    }

    public List<Product> findAll() {
        return new ArrayList<>(store.values());
    }
}
