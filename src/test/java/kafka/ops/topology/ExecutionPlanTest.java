package kafka.ops.topology;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import kafka.ops.topology.actions.access.ClearBindings;
import kafka.ops.topology.actions.access.CreateBindings;
import kafka.ops.topology.actions.topics.DeleteTopics;
import kafka.ops.topology.actions.topics.SyncTopicAction;
import kafka.ops.topology.api.adminclient.TopologyBuilderAdminClient;
import kafka.ops.topology.model.Impl.ProjectImpl;
import kafka.ops.topology.model.Impl.TopicImpl;
import kafka.ops.topology.model.Impl.TopologyImpl;
import kafka.ops.topology.model.Project;
import kafka.ops.topology.model.Topic;
import kafka.ops.topology.model.Topology;
import kafka.ops.topology.roles.SimpleAclsProvider;
import kafka.ops.topology.roles.TopologyAclBinding;
import kafka.ops.topology.schemas.SchemaRegistryManager;
import kafka.ops.topology.utils.TestUtils;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ExecutionPlanTest {

  private ExecutionPlan plan;
  private BackendController backendController;

  @Mock PrintStream mockPrintStream;

  @Mock SimpleAclsProvider aclsProvider;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock TopologyBuilderAdminClient adminClient;

  @Mock SchemaRegistryManager schemaRegistryManager;

  @Before
  public void before() throws IOException {
    TestUtils.deleteStateFile();
    backendController = new BackendController();
    plan = ExecutionPlan.init(backendController, mockPrintStream);
  }

  @Test
  public void addBindingsTest() throws IOException {
    TopologyAclBinding binding1 =
        new TopologyAclBinding(ResourceType.ANY, "topicA", "*", "ALL", "User:foo", "LITERAL");
    TopologyAclBinding binding2 =
        new TopologyAclBinding(ResourceType.ANY, "topicB", "*", "ALL", "User:foo", "LITERAL");
    Set<TopologyAclBinding> bindings = new HashSet<>(Arrays.asList(binding1, binding2));
    CreateBindings addBindingsAction = new CreateBindings(aclsProvider, bindings);

    plan.add(addBindingsAction);

    plan.run();

    verify(aclsProvider, times(1)).createBindings(bindings);
    assertEquals(2, backendController.size());
  }

  @Test
  public void deleteBindingsAfterCreateTest() throws IOException {
    TopologyAclBinding binding1 =
        new TopologyAclBinding(ResourceType.ANY, "topicA", "*", "ALL", "User:foo", "LITERAL");
    TopologyAclBinding binding2 =
        new TopologyAclBinding(ResourceType.ANY, "topicB", "*", "ALL", "User:foo", "LITERAL");
    Set<TopologyAclBinding> bindings = new HashSet<>(Arrays.asList(binding1, binding2));
    CreateBindings addBindingsAction = new CreateBindings(aclsProvider, bindings);

    plan.add(addBindingsAction);

    plan.run();

    verify(aclsProvider, times(1)).createBindings(bindings);
    assertEquals(2, backendController.size());

    BackendController backendController = new BackendController();
    ExecutionPlan plan = ExecutionPlan.init(backendController, mockPrintStream);

    bindings = new HashSet<>(singletonList(binding2));
    ClearBindings clearBindingsAction = new ClearBindings(aclsProvider, bindings);

    plan.add(clearBindingsAction);

    plan.run();

    verify(aclsProvider, times(1)).clearBindings(bindings);
    assertEquals(1, backendController.size());

    backendController = new BackendController();
    backendController.load();
    assertEquals(1, backendController.size());
  }

  @Test
  public void addTopicsTest() throws IOException {
    Topology topology = buildTopologyForTest();
    Topic topicFoo = topology.getProjects().get(0).getTopics().get(0);
    Topic topicBar = topology.getProjects().get(0).getTopics().get(1);
    Set<String> listOfTopics = new HashSet<>();

    SyncTopicAction addTopicAction1 =
        new SyncTopicAction(
            adminClient, schemaRegistryManager, topicFoo, topicFoo.toString(), listOfTopics);

    SyncTopicAction addTopicAction2 =
        new SyncTopicAction(
            adminClient, schemaRegistryManager, topicBar, topicBar.toString(), listOfTopics);

    plan.add(addTopicAction1);
    plan.add(addTopicAction2);

    plan.run();

    verify(adminClient, times(1)).createTopic(topicFoo, topicFoo.toString());
    verify(adminClient, times(1)).createTopic(topicBar, topicBar.toString());
    assertEquals(2, backendController.size());
  }

  @Test
  public void deleteTopicsPreviouslyAddedTest() throws IOException {
    Topology topology = buildTopologyForTest();
    Topic topicFoo = topology.getProjects().get(0).getTopics().get(0);
    Topic topicBar = topology.getProjects().get(0).getTopics().get(1);
    Set<String> listOfTopics = new HashSet<>();

    SyncTopicAction addTopicAction1 =
        new SyncTopicAction(
            adminClient, schemaRegistryManager, topicFoo, topicFoo.toString(), listOfTopics);

    SyncTopicAction addTopicAction2 =
        new SyncTopicAction(
            adminClient, schemaRegistryManager, topicBar, topicBar.toString(), listOfTopics);

    plan.add(addTopicAction1);
    plan.add(addTopicAction2);

    plan.run();

    verify(adminClient, times(1)).createTopic(topicFoo, topicFoo.toString());
    verify(adminClient, times(1)).createTopic(topicBar, topicBar.toString());
    assertEquals(2, backendController.size());

    BackendController backendController = new BackendController();
    ExecutionPlan plan = ExecutionPlan.init(backendController, mockPrintStream);

    DeleteTopics deleteTopicsAction =
        new DeleteTopics(adminClient, singletonList(topicFoo.toString()));
    plan.add(deleteTopicsAction);

    plan.run();

    verify(adminClient, times(1)).deleteTopics(singletonList(topicFoo.toString()));
    assertEquals(1, backendController.size());
  }

  private Topology buildTopologyForTest() {
    Topology topology = new TopologyImpl();
    topology.setContext("context");
    Project project = new ProjectImpl("project");
    topology.setProjects(singletonList(project));

    Topic topic = new TopicImpl("foo");
    Topic topicBar = new TopicImpl("bar");
    project.setTopics(Arrays.asList(topic, topicBar));
    return topology;
  }
}