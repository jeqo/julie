package com.purbon.kafka.topology.utils;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

public class ZkClient {

  private static final Logger LOGGER = LogManager.getLogger(ZkClient.class);

  private ZkConnection connection;
  private ZooKeeper zkClient;

  public ZkClient() {
    connection = new ZkConnection();
  }

  public void connect(String host) throws IOException, InterruptedException {
    zkClient = connection.connect(host);
  }

  public String getNodeData(String path) throws IOException {
    try {
      byte[] data = zkClient.getData(path, null, null);
      return new String(data);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new IOException(e);
    }
  }
}
