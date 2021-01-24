package kafka.ops.topology.actions.topics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import kafka.ops.topology.actions.BaseAction;
import kafka.ops.topology.api.adminclient.TopologyBuilderAdminClient;
import kafka.ops.topology.model.Topic;
import kafka.ops.topology.model.TopicSchemas;
import kafka.ops.topology.model.schema.Subject;
import kafka.ops.topology.schemas.SchemaRegistryManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SyncTopicAction extends BaseAction {

  private static final Logger LOGGER = LogManager.getLogger(SyncTopicAction.class);

  private final Topic topic;
  private final String fullTopicName;
  private final Set<String> listOfTopics;
  private final TopologyBuilderAdminClient adminClient;
  private final SchemaRegistryManager schemaRegistryManager;

  public SyncTopicAction(
      TopologyBuilderAdminClient adminClient,
      SchemaRegistryManager schemaRegistryManager,
      Topic topic,
      String fullTopicName,
      Set<String> listOfTopics) {
    this.topic = topic;
    this.fullTopicName = fullTopicName;
    this.listOfTopics = listOfTopics;
    this.adminClient = adminClient;
    this.schemaRegistryManager = schemaRegistryManager;
  }

  public String getTopic() {
    return fullTopicName;
  }

  @Override
  public void run() throws IOException {
    syncTopic(topic, fullTopicName, listOfTopics);
  }

  private void syncTopic(Topic topic, String fullTopicName, Set<String> listOfTopics)
      throws IOException {
    LOGGER.debug(String.format("Sync topic %s", fullTopicName));
    if (existTopic(fullTopicName, listOfTopics)) {
      if (topic.partitionsCount() > adminClient.getPartitionCount(fullTopicName)) {
        LOGGER.debug(String.format("Update partition count of topic %s", fullTopicName));
        adminClient.updatePartitionCount(topic, fullTopicName);
      }
      adminClient.updateTopicConfig(topic, fullTopicName);
    } else {
      LOGGER.debug(String.format("Create new topic with name %s", fullTopicName));
      adminClient.createTopic(topic, fullTopicName);
    }

    for (TopicSchemas schema : topic.getSchemas()) {
      Subject keySubject = schema.getKeySubject();
      Subject valueSubject = schema.getValueSubject();
      if (keySubject.hasSchemaFile()) {
        String keySchemaFile = keySubject.getSchemaFile();
        String subjectName = keySubject.buildSubjectName(topic);
        schemaRegistryManager.register(subjectName, keySchemaFile, keySubject.getFormat());
        setCompatibility(subjectName, keySubject.getOptionalCompatibility());
      }
      if (valueSubject.hasSchemaFile()) {
        String valueSchemaFile = valueSubject.getSchemaFile();
        String subjectName = valueSubject.buildSubjectName(topic);
        schemaRegistryManager.register(subjectName, valueSchemaFile, valueSubject.getFormat());
        setCompatibility(subjectName, valueSubject.getOptionalCompatibility());
      }
    }
  }

  private void setCompatibility(String subjectName, Optional<String> compatibilityOptional) {
    compatibilityOptional.ifPresent(
        compatibility -> schemaRegistryManager.setCompatibility(subjectName, compatibility));
  }

  private boolean existTopic(String topic, Set<String> listOfTopics) {
    return listOfTopics.contains(topic);
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new HashMap<>();
    map.put("Operation", getClass().getName());
    map.put("Topic", fullTopicName);
    String actionName = existTopic(fullTopicName, listOfTopics) ? "update" : "create";
    map.put("Action", actionName);
    return map;
  }
}