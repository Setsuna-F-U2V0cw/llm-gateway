package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class CircuitBreakerServiceTest {

    @Test
    void namedBreakers_areIndependent_andOpenShortCircuits() {
        GatewayProperties props = new GatewayProperties();
        props.getCb().setSlidingWindowSize(4);
        props.getCb().setMinimumNumberOfCalls(2);
        props.getCb().setFailureRateThreshold(50f);
        props.getCb().setWaitDurationInOpenStateMs(60_000);
        CircuitBreakerService service = new CircuitBreakerService(props, new SimpleMeterRegistry());

        CircuitBreakerOperator<String> modelA = service.get("model-a");
        CircuitBreakerOperator<String> modelB = service.get("model-b");

        StepVerifier.create(Flux.<String>error(new RuntimeException("fail")).transform(modelA))
                .verifyError(RuntimeException.class);
        StepVerifier.create(Flux.<String>error(new RuntimeException("fail")).transform(modelA))
                .verifyError(RuntimeException.class);

        StepVerifier.create(Flux.just("ok").transform(modelA))
                .verifyError(CallNotPermittedException.class);

        StepVerifier.create(Flux.just("ok").transform(modelB))
                .expectNext("ok")
                .verifyComplete();
    }

    @Test
    void slowCallJudgement_isEffectivelyDisabled() throws Exception {
        CircuitBreakerService service = new CircuitBreakerService(
                new GatewayProperties(), new SimpleMeterRegistry());
        service.get("m1");

        Field field = CircuitBreakerService.class.getDeclaredField("registry");
        field.setAccessible(true);
        CircuitBreakerRegistry registry = (CircuitBreakerRegistry) field.get(service);
        CircuitBreakerConfig cfg = registry.circuitBreaker("m1").getCircuitBreakerConfig();

        assertThat(cfg.getSlowCallRateThreshold()).isEqualTo(100f);
        assertThat(cfg.getSlowCallDurationThreshold()).isEqualTo(Duration.ofDays(36500));
        assertThat(cfg.getRecordExceptionPredicate().test(new TimeoutException())).isTrue();
    }
}
