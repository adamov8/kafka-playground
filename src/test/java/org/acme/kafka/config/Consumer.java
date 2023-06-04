package org.acme.kafka.config;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;


@Slf4j
@ApplicationScoped
public class Consumer {

  @Channel("messages-processed")
  Emitter<String> processed;

  @Getter
  List<String> messagesProcessed = new ArrayList<>();

  @Incoming(value = "messages-in")
  public void processAndSend(String message) {
    log.info("Consumed message: {}", message);

    log.info("Sending to processed: {}", message);
    processed.send(message);
  }

  @Incoming(value = "messages-in-2")
  public void process(String message) {
    log.info("Consumed message: {}", message);

    messagesProcessed.add(message);

    log.info("Sending to processed: {}", message);
  }
}
