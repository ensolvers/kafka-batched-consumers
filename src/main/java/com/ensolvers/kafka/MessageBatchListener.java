package com.ensolvers.kafka;

import java.util.List;

public interface MessageBatchListener {
  
  void onMessageBatch(List<String> messages);
}

