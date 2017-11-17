package com.ensolvers.kafka;

public class KafkaWriterBuilder {
  
  public static KafkaWriterBuilder writerOn(String producersUrl, String topic) {
    return new KafkaWriterBuilder(producersUrl, topic);
  }
  
  private String producersUrl;
  private String topic;
  
  private KafkaWriterBuilder(String producersUrl, String topic) {
    this.producersUrl = producersUrl;
    this.topic = topic;
  }
  
  public KafkaWriter build() {
    return new KafkaWriter(this.producersUrl, this.topic);
  }
  
}
