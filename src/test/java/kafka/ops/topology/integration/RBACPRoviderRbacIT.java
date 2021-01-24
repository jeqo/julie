package kafka.ops.topology.integration;

import static kafka.ops.topology.roles.rbac.RBACPredefinedRoles.DEVELOPER_READ;
import static kafka.ops.topology.roles.rbac.RBACPredefinedRoles.DEVELOPER_WRITE;
import static kafka.ops.topology.roles.rbac.RBACPredefinedRoles.RESOURCE_OWNER;
import static kafka.ops.topology.roles.rbac.RBACPredefinedRoles.SECURITY_ADMIN;
import static kafka.ops.topology.roles.rbac.RBACPredefinedRoles.SYSTEM_ADMIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import kafka.ops.topology.AccessControlManager;
import kafka.ops.topology.BackendController;
import kafka.ops.topology.BuilderCli;
import kafka.ops.topology.ExecutionPlan;
import kafka.ops.topology.TopologyBuilderConfig;
import kafka.ops.topology.api.mds.MdsApiClient;
import kafka.ops.topology.model.Impl.ProjectImpl;
import kafka.ops.topology.model.Impl.TopicImpl;
import kafka.ops.topology.model.Impl.TopologyImpl;
import kafka.ops.topology.model.Platform;
import kafka.ops.topology.model.Project;
import kafka.ops.topology.model.Topic;
import kafka.ops.topology.model.Topology;
import kafka.ops.topology.model.User;
import kafka.ops.topology.model.users.Connector;
import kafka.ops.topology.model.users.Consumer;
import kafka.ops.topology.model.users.KStream;
import kafka.ops.topology.model.users.Producer;
import kafka.ops.topology.model.users.platform.ControlCenter;
import kafka.ops.topology.model.users.platform.ControlCenterInstance;
import kafka.ops.topology.model.users.platform.Kafka;
import kafka.ops.topology.model.users.platform.KafkaConnect;
import kafka.ops.topology.model.users.platform.SchemaRegistry;
import kafka.ops.topology.model.users.platform.SchemaRegistryInstance;
import kafka.ops.topology.roles.RbacProvider;
import kafka.ops.topology.roles.rbac.RBACBindingsBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RBACPRoviderRbacIT extends MDSBaseTest {

  private String mdsServer = "http://localhost:8090";
  private String mdsUser = "professor";
  private String mdsPassword = "professor";

  private MdsApiClient apiClient;
  @Mock private BackendController cs;
  private ExecutionPlan plan;

  private AccessControlManager accessControlManager;

  @Before
  public void before() throws IOException, InterruptedException {
    super.beforeEach();
    apiClient = new MdsApiClient(mdsServer);
    apiClient.login(mdsUser, mdsPassword);
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());
    apiClient.setSchemaRegistryClusterID(getSchemaRegistryClusterID());
    apiClient.setConnectClusterID(getKafkaConnectClusterID());

    plan = ExecutionPlan.init(cs, System.out);
    RbacProvider rbacProvider = new RbacProvider(apiClient);
    RBACBindingsBuilder bindingsBuilder = new RBACBindingsBuilder(apiClient);

    Properties props = new Properties();

    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BuilderCli.BROKERS_OPTION, "");
    cliOps.put(BuilderCli.ALLOW_DELETE_OPTION, "true");

    TopologyBuilderConfig config = new TopologyBuilderConfig(cliOps, props);

    accessControlManager = new AccessControlManager(rbacProvider, bindingsBuilder, config);
  }

  @Test
  public void consumerAclsCreation() throws IOException {

    List<Consumer> consumers = new ArrayList<>();
    consumers.add(new Consumer("User:app1"));

    Project project = new ProjectImpl("project");
    project.setConsumers(consumers);
    Topic topicA = new TopicImpl("topicA");
    project.addTopic(topicA);

    Topology topology = new TopologyImpl();
    topology.setContext("testConsumerAclsCreation-test");
    topology.addProject(project);

    accessControlManager.apply(topology, plan);
    plan.run();

    // this method is call twice, once for consumers and one for producers
    verify(cs, times(1)).addBindings(anyList());
    verify(cs, times(1)).flushAndClose();
    verifyConsumerAcls(consumers, topicA.toString());
  }

  @Test
  public void producerAclsCreation() throws IOException {

    List<Producer> producers = new ArrayList<>();
    producers.add(new Producer("User:app2"));

    Project project = new ProjectImpl("project");
    project.setProducers(producers);
    Topic topicA = new TopicImpl("topicA");
    project.addTopic(topicA);

    Topology topology = new TopologyImpl();
    topology.setContext("producerAclsCreation-test");
    topology.addProject(project);

    accessControlManager.apply(topology, plan);
    plan.run();

    // this method is call twice, once for consumers and one for consumers
    verify(cs, times(1)).addBindings(anyList());
    verify(cs, times(1)).flushAndClose();
    verifyProducerAcls(producers, topicA.toString());
  }

  @Test
  public void kstreamsAclsCreation() throws IOException {
    Project project = new ProjectImpl();

    KStream app = new KStream();
    app.setPrincipal("User:App3");
    HashMap<String, List<String>> topics = new HashMap<>();
    topics.put(KStream.READ_TOPICS, Arrays.asList("topicA", "topicB"));
    topics.put(KStream.WRITE_TOPICS, Arrays.asList("topicC", "topicD"));
    app.setTopics(topics);
    project.setStreams(Collections.singletonList(app));

    Topology topology = new TopologyImpl();
    topology.setContext("kstreamsAclsCreation-test");
    topology.addProject(project);

    accessControlManager.apply(topology, plan);
    plan.run();

    verify(cs, times(1)).addBindings(anyList());
    verify(cs, times(1)).flushAndClose();
    verifyKStreamsAcls(app);
  }

  @Test
  public void connectAclsCreation() throws IOException {
    Project project = new ProjectImpl();

    Connector connector = new Connector();
    connector.setPrincipal("User:Connect");
    HashMap<String, List<String>> topics = new HashMap<>();
    topics.put(KStream.READ_TOPICS, Arrays.asList("topicA", "topicB"));
    connector.setTopics(topics);
    project.setConnectors(Collections.singletonList(connector));

    Topology topology = new TopologyImpl();
    topology.setContext("connectAclsCreation-test");
    topology.addProject(project);

    accessControlManager.apply(topology, plan);
    plan.run();

    verify(cs, times(1)).addBindings(anyList());
    verify(cs, times(1)).flushAndClose();
    verifyConnectAcls(connector);
  }

  @Test
  public void schemaRegistryAclsCreation() throws IOException {
    Project project = new ProjectImpl();

    Topology topology = new TopologyImpl();
    topology.setContext("schemaRegistryAclsCreation-test");
    topology.addProject(project);

    Platform platform = new Platform();
    SchemaRegistry sr = new SchemaRegistry();
    SchemaRegistryInstance instance = new SchemaRegistryInstance();
    instance.setPrincipal("User:foo");

    SchemaRegistryInstance instance2 = new SchemaRegistryInstance();
    instance2.setPrincipal("User:banana");

    sr.setInstances(Arrays.asList(instance, instance2));

    Map<String, List<User>> rbac = new HashMap<>();
    rbac.put("SecurityAdmin", Collections.singletonList(new User("User:foo")));
    rbac.put("ClusterAdmin", Collections.singletonList(new User("User:bar")));
    sr.setRbac(Optional.of(rbac));

    platform.setSchemaRegistry(sr);
    topology.setPlatform(platform);

    accessControlManager.apply(topology, plan);
    plan.run();

    verify(cs, times(1)).addBindings(anyList());
    verify(cs, times(1)).flushAndClose();
    verifySchemaRegistryAcls(platform);
  }

  @Test
  public void controlcenterAclsCreation() throws IOException {
    Project project = new ProjectImpl();

    Topology topology = new TopologyImpl();
    topology.setContext("controlcenterAclsCreation-test");
    topology.addProject(project);

    Platform platform = new Platform();
    ControlCenter c3 = new ControlCenter();
    ControlCenterInstance instance = new ControlCenterInstance();
    instance.setPrincipal("User:foo");
    instance.setAppId("appid");
    c3.setInstances(Collections.singletonList(instance));
    platform.setControlCenter(c3);

    topology.setPlatform(platform);

    accessControlManager.apply(topology, plan);
    plan.run();

    verify(cs, times(1)).addBindings(anyList());
    verify(cs, times(1)).flushAndClose();
    verifyControlCenterAcls(platform);
  }

  @Test
  public void kafkaClusterLevelAclCreation() throws IOException {
    Project project = new ProjectImpl();

    Topology topology = new TopologyImpl();
    topology.setContext("kafkaClusterLevelAclCreation-test");
    topology.addProject(project);

    Platform platform = new Platform();
    Kafka kafka = new Kafka();
    Map<String, List<User>> rbac = new HashMap<>();
    rbac.put("Operator", Collections.singletonList(new User("User:foo")));
    rbac.put("ClusterAdmin", Collections.singletonList(new User("User:bar")));
    kafka.setRbac(Optional.of(rbac));
    platform.setKafka(kafka);
    topology.setPlatform(platform);

    accessControlManager.apply(topology, plan);
    plan.run();

    verify(cs, times(1)).addBindings(anyList());
    verify(cs, times(1)).flushAndClose();

    verifyKafkaClusterACLs(platform);
  }

  @Test
  public void connectClusterLevelAclCreation() throws IOException {
    Project project = new ProjectImpl();

    Topology topology = new TopologyImpl();
    topology.setContext("kafkaClusterLevelAclCreation-test");
    topology.addProject(project);

    Platform platform = new Platform();
    KafkaConnect connect = new KafkaConnect();
    Map<String, List<User>> rbac = new HashMap<>();
    rbac.put("Operator", Collections.singletonList(new User("User:foo")));
    rbac.put("ClusterAdmin", Collections.singletonList(new User("User:bar")));
    connect.setRbac(Optional.of(rbac));
    platform.setKafkaConnect(connect);
    topology.setPlatform(platform);

    accessControlManager.apply(topology, plan);
    plan.run();

    verify(cs, times(1)).addBindings(anyList());
    verify(cs, times(1)).flushAndClose();

    verifyConnectClusterACLs(platform);
  }

  private void verifyConnectClusterACLs(Platform platform) {
    Map<String, Map<String, String>> clusters =
        apiClient.withClusterIDs().forKafka().forKafkaConnect().asMap();

    Map<String, List<User>> rbac = platform.getKafkaConnect().getRbac().get();
    for (String role : rbac.keySet()) {
      User user = rbac.get(role).get(0);
      List<String> roles = apiClient.lookupRoles(user.getPrincipal(), clusters);
      assertTrue(roles.contains(role));
    }
  }

  private void verifyKafkaClusterACLs(Platform platform) {
    Map<String, List<User>> kafkaRbac = platform.getKafka().getRbac().get();
    for (String role : kafkaRbac.keySet()) {
      User user = kafkaRbac.get(role).get(0);
      List<String> roles = apiClient.lookupRoles(user.getPrincipal());
      assertTrue(roles.contains(role));
    }
  }

  private void verifyControlCenterAcls(Platform platform) {
    ControlCenterInstance c3 = platform.getControlCenter().getInstances().get(0);
    List<String> roles = apiClient.lookupRoles(c3.getPrincipal());
    assertTrue(roles.contains(SYSTEM_ADMIN));
  }

  private void verifySchemaRegistryAcls(Platform platform) {
    SchemaRegistryInstance sr = platform.getSchemaRegistry().getInstances().get(0);
    List<String> roles = apiClient.lookupRoles(sr.getPrincipal());
    assertTrue(roles.contains(RESOURCE_OWNER));

    Map<String, Map<String, String>> clusters =
        apiClient.withClusterIDs().forKafka().forSchemaRegistry().asMap();

    roles = apiClient.lookupRoles(sr.getPrincipal(), clusters);
    assertTrue(roles.contains(SECURITY_ADMIN));

    Map<String, List<User>> srRbac = platform.getSchemaRegistry().getRbac().get();
    for (String role : srRbac.keySet()) {
      User user = srRbac.get(role).get(0);
      roles = apiClient.lookupRoles(user.getPrincipal(), clusters);
      assertTrue(roles.contains(role));
    }
  }

  private void verifyConnectAcls(Connector app) {
    List<String> roles = apiClient.lookupRoles(app.getPrincipal());
    assertTrue(roles.contains(DEVELOPER_READ));
    assertTrue(roles.contains(RESOURCE_OWNER));

    Map<String, Map<String, String>> clusters =
        apiClient.withClusterIDs().forKafka().forKafkaConnect().asMap();

    roles = apiClient.lookupRoles(app.getPrincipal(), clusters);
    assertTrue(roles.contains(SECURITY_ADMIN));
  }

  private void verifyKStreamsAcls(KStream app) {
    List<String> roles = apiClient.lookupRoles(app.getPrincipal());
    assertTrue(roles.contains(DEVELOPER_READ));
    assertTrue(roles.contains(DEVELOPER_WRITE));
    assertTrue(roles.contains(RESOURCE_OWNER));
  }

  private void verifyProducerAcls(List<Producer> producers, String topic) {
    producers.forEach(
        producer -> {
          List<String> roles = apiClient.lookupRoles(producer.getPrincipal());
          assertEquals(1, roles.size());
          assertTrue(roles.contains(DEVELOPER_WRITE));
        });
  }

  private void verifyConsumerAcls(List<Consumer> consumers, String topic) {
    consumers.forEach(
        consumer -> {
          List<String> roles = apiClient.lookupRoles(consumer.getPrincipal());
          assertEquals(2, roles.size());
          assertTrue(roles.contains(DEVELOPER_READ));
          assertTrue(roles.contains(RESOURCE_OWNER));
        });
  }
}