package com.github.seanyinx.mq.runner.rocketmq;

import static org.apache.rocketmq.client.consumer.PullStatus.FOUND;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMqPullConsumerRunner implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pull-consumer-runner-" + System.currentTimeMillis());

  private void run(String mqAddress, String topic, String tag) throws MQClientException, InterruptedException, RemotingException, MQBrokerException {
    consumer.setNamesrvAddr(mqAddress);
    consumer.setMessageModel(MessageModel.BROADCASTING);
    consumer.setInstanceName(UUID.randomUUID().toString());
    consumer.start();

    Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues(topic);
    LOGGER.info("queues: {}", messageQueues);

    while (!messageQueues.isEmpty()) {
      for (Iterator<MessageQueue> iterator = messageQueues.iterator(); iterator.hasNext(); ) {
        MessageQueue messageQueue = iterator.next();
        long offset = consumer.fetchConsumeOffset(messageQueue, false);
        if (offset >= consumer.maxOffset(messageQueue)) {
          LOGGER.info("Finished pulling from queue {}", messageQueue);
          iterator.remove();
          continue;
        }

        long startTime = System.nanoTime();
        PullResult result = consumer.pull(messageQueue, tag, offset < 0 ? consumer.minOffset(messageQueue) : offset, 32);
        LOGGER.info("Pulling spent {} nano seconds on queue {}", System.nanoTime() - startTime, messageQueue);

        consumer.getDefaultMQPullConsumerImpl().updateConsumeOffsetToBroker(messageQueue, result.getNextBeginOffset(), true);

        LOGGER.info("Pulling result {}", result);
        if (FOUND.equals(result.getPullStatus())) {
          LOGGER.info("MQ: msg size = {}", result.getMsgFoundList().size());
        }
      }
    }
  }

  @Override
  public void close() {
    consumer.shutdown();
  }

  public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
    try (RocketMqPullConsumerRunner runner = new RocketMqPullConsumerRunner()) {
      runner.run(mqAddress(), topic(), tag());
    }
  }

  private static String mqAddress() {
    return System.getProperty("mq.address", "localhost:9876");
  }

  private static String topic() {
    return System.getProperty("mq.topic", "topic");
  }

  private static String tag() {
    return System.getProperty("mq.tag", "");
  }
}