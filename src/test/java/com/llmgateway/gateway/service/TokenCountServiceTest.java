package com.llmgateway.gateway.service;

import com.llmgateway.gateway.model.ChatRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TokenCountServiceTest {

    private final TokenCountService service = new TokenCountService();

    @Test
    void actualTotalTokens_includesInputAndOutput() {
        ChatRequest req = new ChatRequest();
        ChatRequest.Message m = new ChatRequest.Message();
        m.setRole("user");
        m.setContent("hello");
        req.setMessages(List.of(m));

        int input = service.countInputTokens(req);
        int output = service.countTokens("world");
        int estimated = service.estimateTotalTokens(req);

        assertThat(service.actualTotalTokens(req, "world")).isEqualTo(input + output);
        // 预扣 = input + 1.5*input，短输出时 estimated > actual，对应「多退」
        assertThat(estimated).isGreaterThan(service.actualTotalTokens(req, "world"));
    }

    @Test
    void actualTotalTokens_blankOutput_isJustInput() {
        ChatRequest req = new ChatRequest();
        ChatRequest.Message m = new ChatRequest.Message();
        m.setRole("user");
        m.setContent("hello");
        req.setMessages(List.of(m));

        assertThat(service.actualTotalTokens(req, "")).isEqualTo(service.countInputTokens(req));
        assertThat(service.actualTotalTokens(req, null)).isEqualTo(service.countInputTokens(req));
    }

    @Test
    void estimateTotalTokens_capsOutputByMaxTokens() {
        ChatRequest req = new ChatRequest();
        ChatRequest.Message m = new ChatRequest.Message();
        m.setRole("user");
        m.setContent("hello");
        req.setMessages(List.of(m));
        req.setMaxTokens(1);

        int input = service.countInputTokens(req);
        assertThat(service.estimateTotalTokens(req)).isEqualTo(input + 1);
    }

    @Test
    void countInputTokens_emptyOrNullMessages_isZero() {
        ChatRequest empty = new ChatRequest();
        empty.setMessages(List.of());
        assertThat(service.countInputTokens(empty)).isEqualTo(0);
        assertThat(service.estimateTotalTokens(empty)).isEqualTo(0);
        assertThat(service.countInputTokens(new ChatRequest())).isEqualTo(0);
        assertThat(service.countTokens("   ")).isEqualTo(0);
    }

    @Test
    void countInputTokens_includesPerMessageOverhead() {
        ChatRequest req = new ChatRequest();
        ChatRequest.Message sys = new ChatRequest.Message();
        sys.setRole("system");
        sys.setContent("you are helpful");
        ChatRequest.Message user = new ChatRequest.Message();
        user.setRole("user");
        user.setContent("hi");
        req.setMessages(List.of(sys, user));

        int one = service.countInputTokens(requestHello());
        int two = service.countInputTokens(req);
        assertThat(two).isGreaterThan(one);
    }

    private ChatRequest requestHello() {
        ChatRequest req = new ChatRequest();
        ChatRequest.Message m = new ChatRequest.Message();
        m.setRole("user");
        m.setContent("hello");
        req.setMessages(List.of(m));
        return req;
    }
}
