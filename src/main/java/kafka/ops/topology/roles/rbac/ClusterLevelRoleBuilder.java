package kafka.ops.topology.roles.rbac;

import java.util.Map;
import java.util.Optional;
import kafka.ops.topology.api.mds.ClusterIds;
import kafka.ops.topology.api.mds.MdsApiClient;
import kafka.ops.topology.api.mds.RequestScope;
import kafka.ops.topology.model.users.Connector;
import kafka.ops.topology.roles.TopologyAclBinding;
import org.apache.kafka.common.resource.PatternType;

public class ClusterLevelRoleBuilder {

  private final String principal;
  private final String role;
  private final MdsApiClient client;
  private RequestScope scope;

  public ClusterLevelRoleBuilder(String principal, String role, MdsApiClient client) {
    this.principal = principal;
    this.role = role;
    this.client = client;
    this.scope = new RequestScope();
  }

  public ClusterLevelRoleBuilder forSchemaRegistry() {
    Map<String, Map<String, String>> clusters =
        client.withClusterIDs().forSchemaRegistry().forKafka().asMap();

    scope = new RequestScope();
    scope.setClusters(clusters);
    scope.build();

    return this;
  }

  public ClusterLevelRoleBuilder forSchemaSubject(String subject) {
    Map<String, Map<String, String>> clusters =
        client.withClusterIDs().forSchemaRegistry().forKafka().asMap();

    scope = new RequestScope();
    scope.setClusters(clusters);
    scope.addResource("Subject", "Subject:" + subject, PatternType.LITERAL.name());
    scope.build();

    return this;
  }

  public ClusterLevelRoleBuilder forAKafkaConnector(String connector) {
    Map<String, Map<String, String>> clusters =
        client.withClusterIDs().forKafkaConnect().forKafka().asMap();

    String patternType = PatternType.LITERAL.name();

    scope = new RequestScope();
    scope.setClusters(clusters);
    scope.addResource("Connector", "Connector:" + connector, patternType);
    scope.build();

    return this;
  }

  public TopologyAclBinding apply() {

    return client.bindClusterRole(principal, role, scope);
  }

  public ClusterLevelRoleBuilder forKafka() {
    Map<String, Map<String, String>> clusters = client.withClusterIDs().forKafka().asMap();

    scope = new RequestScope();
    scope.setClusters(clusters);
    scope.build();

    return this;
  }

  public ClusterLevelRoleBuilder forControlCenter() {
    Map<String, Map<String, String>> clusters = client.withClusterIDs().forKafka().asMap();

    scope = new RequestScope();
    scope.setClusters(clusters);
    scope.addResource("Cluster", "control-center", PatternType.LITERAL.name());
    scope.build();

    return this;
  }

  public ClusterLevelRoleBuilder forKafkaConnect() {
    Map<String, Map<String, String>> clusters =
        client.withClusterIDs().forKafkaConnect().forKafka().asMap();

    scope = new RequestScope();
    scope.setClusters(clusters);
    scope.addResource("Cluster", "kafka-connect", PatternType.LITERAL.name());

    scope.build();

    return this;
  }

  public ClusterLevelRoleBuilder forKafkaConnect(Connector connector) {
    Map<String, Map<String, String>> clusters =
        client.withClusterIDs().forKafkaConnect().forKafka().asMap();

    Optional<String> connectClusterIdOptional = connector.getCluster_id();
    connectClusterIdOptional.ifPresent(
        s -> clusters.get("clusters").put(ClusterIds.CONNECT_CLUSTER_ID_LABEL, s));

    scope = new RequestScope();
    scope.setClusters(clusters);
    scope.addResource("Cluster", "kafka-connect", PatternType.LITERAL.name());

    scope.build();

    return this;
  }

  public RequestScope getScope() {
    return scope;
  }
}