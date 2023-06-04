package org.acme.kafka;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySink;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;

import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.acme.kafka.config.Consumer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
@QuarkusTest
@QuarkusTestResource(KafkaTestResourceLifecycleManager.class)
public class ConsumerTest {

  @Inject
  Consumer consumer;

  @Inject
  @Any
  InMemoryConnector connector;

  private InMemorySink<String> messagesProcessed;

  @AfterEach
  void tearDown() {
    if (messagesProcessed != null) {
      messagesProcessed.clear();
    }
  }

  @Test
  void testProcess() {
    messagesProcessed = connector.sink("messages-processed");

    String hello = "Hello";
    log.info("Sending message {}", hello);
    consumer.process(hello);

    Assertions.assertEquals(hello, messagesProcessed.received().get(0).getPayload());
    messagesProcessed.clear();
  }

  @Test
  void testProcessList() {
    InMemorySink<String> messagesProcessed = connector.sink("messages-processed");

    List<String> messages = IntStream.range(1, 11)
        .boxed()
        .map(String::valueOf)
        .toList();
    log.info("Sending messages: {}", messages);
    messages.forEach(consumer::process);

    List<String> processedMessage = messagesProcessed.received().stream()
        .map(Message::getPayload)
        .toList();
    Assertions.assertIterableEquals(messages, processedMessage);
    messagesProcessed.clear();
  }
}
