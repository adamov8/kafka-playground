package org.acme.kafka.config;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;


@Slf4j
@ApplicationScoped
public class Consumer {

  @Channel("messages-processed")
  Emitter<String> processed;

  @Incoming(value = "messages-in")
  public void process(String message) {
    log.info("Consumed message: {}", message);

    log.info("Sending to processed: {}", message);
    processed.send(message);
  }
}
