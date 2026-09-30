package com.github.guoyyit.deadletterqueuejava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(DlqProperties.class)
public class DlqConfiguration {
}
