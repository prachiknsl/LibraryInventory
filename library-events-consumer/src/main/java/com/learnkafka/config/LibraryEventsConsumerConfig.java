package com.learnkafka.config;

import com.learnkafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareBatchErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Autowired
    LibraryEventService libraryEventService;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory
            (ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
             ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        //Customize Handling
        factory.setErrorHandler(((throwException, data) -> {
            log.error("Exception in consumerConfig is {}",throwException.getMessage(),data);
        }));
        factory.setRetryTemplate(retryTemplate());
        factory.setRecoveryCallback((context ->{
            if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException)
            {
                //invoke recovery
                log.info("Inside Recoverable logic");
//                Arrays.asList(context.attributeNames())
//                        .forEach(attribute -> {
//                            log.info("Attribute name is : {}, context {} ", attribute, context.getAttribute(attribute));
//                        });
                ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute("record");
                libraryEventService.handleRecovery(consumerRecord);
            }
            else {
                log.error("Inside non-recoverable logic");
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }
            return null;
        }));
        return factory;
    }

    private RetryTemplate retryTemplate() {

        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
//        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
//        simpleRetryPolicy.setMaxAttempts(3);
        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();
        exceptionMap.put(IllegalArgumentException.class, false);
        exceptionMap.put(RecoverableDataAccessException.class, true);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3,exceptionMap, true);
        return simpleRetryPolicy;
    }


}
