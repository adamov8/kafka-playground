package org.acme.kafka;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySink;
import io.smallrye.reactive.messaging.memory.InMemorySource;
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
  void testProcessAndSend() {
    messagesProcessed = connector.sink("messages-processed");

    String hello = "Hello";
    log.info("Sending message {}", hello);
    consumer.processAndSend(hello);

    Assertions.assertEquals(hello, messagesProcessed.received().get(0).getPayload());
    messagesProcessed.clear();
  }

  @Test
  void testProcessAndSendList() {
    InMemorySink<String> messagesProcessed = connector.sink("messages-processed");

    List<String> messages = IntStream.range(1, 11)
        .boxed()
        .map(String::valueOf)
        .toList();
    log.info("Sending messages: {}", messages);
    messages.forEach(consumer::processAndSend);

    List<String> processedMessage = messagesProcessed.received().stream()
        .map(Message::getPayload)
        .toList();
    Assertions.assertIterableEquals(messages, processedMessage);
    messagesProcessed.clear();
  }

  @Test
  void testProcess() {
    InMemorySource<String> messagesIn = connector.source("messages-in-2");
    List<String> messages = IntStream.range(1, 11)
        .boxed()
        .map(String::valueOf)
        .toList();
    log.info("Sending messages: {}", messages);

    messages.forEach(messagesIn::send);

    Assertions.assertIterableEquals(messages, consumer.getMessagesProcessed());
  }
}
