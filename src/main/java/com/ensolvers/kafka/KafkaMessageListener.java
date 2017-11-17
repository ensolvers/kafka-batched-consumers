package com.ensolvers.kafka;

public interface KafkaMessageListener {
  void onMessage(String message);
}
