package com.github.guoyyit.deadletterqueuejava.domain;

import java.util.List;

public record PagedResult<T>(
        List<T> items,
        int page,
        int size,
        long total
) {
}
