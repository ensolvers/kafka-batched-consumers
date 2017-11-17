package com.ensolvers.kafka;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class KafkaReaderBuilderIT {
  private static Logger logger = LoggerFactory.getLogger(KafkaReaderBuilderIT.class);
  
  @Test(timeout = 20000)
  public void testSingle() throws InterruptedException {
    int totalMessages = 100;
    String testTopic = "test-topic-single";
    
    final CountDownLatch latch = new CountDownLatch(totalMessages);
    
    KafkaReaderContext context = KafkaReaderBuilder
                                   .readerOn(testTopic)
                                   .zookeeper("localhost:9092")
                                   .groupId("myGroupSingle")
                                   .numberOfThreads(10)
                                   .batchMaxSize(1)
                                   .listener(new MessageListener() {
                                     @Override
                                     public void onMessage(String message) {
                                       logger.info(message);
                                       latch.countDown();
                                     }
                                   })
                                   .build();
    
    context.start();
    
    KafkaWriter writer = KafkaWriterBuilder.writerOn("localhost:9092", testTopic).build();
    
    for (Integer i = 0; i < totalMessages; i++) {
      writer.write(i.toString(), i.toString());
    }
    
    latch.await();
  }
  
  @Test(timeout = 20000)
  public void testBatch() throws InterruptedException {
    int totalMessages = 100000;
    String testTopic = "testtopic";
    
    final CountDownLatch latch = new CountDownLatch(totalMessages);
    
    KafkaReaderContext context = KafkaReaderBuilder
                                   .readerOn(testTopic)
                                   .zookeeper("localhost:9092")
                                   .groupId("myGroup")
                                   .numberOfThreads(10)
                                   .consumerThreads(6)
                                   .batchMaxSize(500)
                                   .batchWaitAtMost(500, TimeUnit.MILLISECONDS)
                                   .listener(new MessageBatchListener() {
        
                                     @Override
                                     public void onMessageBatch(List<String> messages) {
                                       logger.info(Integer.valueOf(messages.size()).toString());
                                       for (int i = 0; i < messages.size(); i++) {
                                         latch.countDown();
                                       }
                                     }
        
                                   })
                                   .build();
    
    context.start();
    
    KafkaWriter writer = KafkaWriterBuilder.writerOn("localhost:9092", testTopic).build();
    
    for (Integer i = 0; i < totalMessages; i++) {
      writer.write(i.toString(), i.toString());
    }
    
    latch.await();
  }
  
}
