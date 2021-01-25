package com.purbon.kafka.topology.integration;

import com.purbon.kafka.topology.utils.Json;
import com.purbon.kafka.topology.utils.ZkClient;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MdsBaseTest {

  private static final Logger LOGGER = LogManager.getLogger(MdsBaseTest.class);
  private ZkClient zkClient;

  public void beforeEach() throws IOException, InterruptedException {
    zkClient = new ZkClient();
    zkClient.connect("localhost");
  }

  protected String getKafkaClusterID() {

    try {
      String nodeData = zkClient.getNodeData("/cluster/id");
      return Json.toMap(nodeData).get("id").toString();
    } catch (IOException e) {
      LOGGER.error(e);
    }
    return "-1";
  }

  protected String getSchemaRegistryClusterID() {
    return "schema-registry";
  }

  protected String getKafkaConnectClusterID() {
    return "connect-cluster";
  }
}
