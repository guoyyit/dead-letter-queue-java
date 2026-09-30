package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.model.StatsSnapshot;

/**
 * 统计查询，全部由当前记录实时派生。
 */
public interface StatsService {

    StatsSnapshot snapshot();
}
