package com.ensolvers.kafka;

public interface MessageListener {
  
  void onMessage(String message);
}