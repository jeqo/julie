package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.roles.rbac.RbacBindingsBuilder.LITERAL;
import static com.purbon.kafka.topology.roles.rbac.RbacPredefinedRoles.DEVELOPER_READ;
import static com.purbon.kafka.topology.roles.rbac.RbacPredefinedRoles.RESOURCE_OWNER;
import static com.purbon.kafka.topology.roles.rbac.RbacPredefinedRoles.SECURITY_ADMIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.purbon.kafka.topology.api.mds.AuthenticationCredentials;
import com.purbon.kafka.topology.api.mds.MdsApiClient;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MdsApiClientRbacIT extends MdsBaseTest {

  private String mdsUser = "professor";
  private String mdsPassword = "professor";

  private MdsApiClient apiClient;

  @Before
  public void before() throws IOException, InterruptedException {
    String mdsServer = "http://localhost:8090";
    apiClient = new MdsApiClient(mdsServer);
  }

  @Test
  public void testMdsLogin() throws IOException {
    apiClient.login(mdsUser, mdsPassword);
    apiClient.authenticate();
    AuthenticationCredentials credentials = apiClient.getCredentials();
    assertFalse(credentials.getAuthToken().isEmpty());
  }

  @Test(expected = IOException.class)
  public void testWithWrongMdsLogin() throws IOException {
    apiClient.login("wrong-user", "wrong-password");
    apiClient.authenticate();
  }

  @Test
  public void testLookupRoles() throws IOException {
    apiClient.login(mdsUser, mdsPassword);
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());

    List<String> roles = apiClient.lookupRoles("User:fry");
    assertTrue(roles.contains(DEVELOPER_READ));
  }

  @Test
  public void testBindRoleToResource() throws IOException {
    apiClient.login(mdsUser, mdsPassword);
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());

    TopologyAclBinding binding =
        apiClient.bind("User:fry", DEVELOPER_READ, "connect-configs", LITERAL);

    apiClient.bindRequest(binding);

    List<String> roles = apiClient.lookupRoles("User:fry");
    assertEquals(1, roles.size());
    assertTrue(roles.contains(DEVELOPER_READ));
  }

  @Test(expected = IOException.class)
  public void testBindRoleWithoutAuthentication() throws IOException {
    apiClient.setKafkaClusterId(getKafkaClusterID());

    TopologyAclBinding binding =
        apiClient.bind("User:fry", DEVELOPER_READ, "connect-configs", LITERAL);

    apiClient.bindRequest(binding);
  }

  @Test
  public void testBindSecurityAdminRole() throws IOException {
    apiClient.login(mdsUser, mdsPassword);
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());
    apiClient.setSchemaRegistryClusterID("schema-registry");
    String principal = "User:foo" + System.currentTimeMillis();

    TopologyAclBinding binding =
        apiClient.bind(principal, SECURITY_ADMIN).forSchemaRegistry().apply();

    apiClient.bindRequest(binding);

    Map<String, Map<String, String>> clusters =
        apiClient.withClusterIDs().forKafka().forSchemaRegistry().asMap();

    List<String> roles = apiClient.lookupRoles(principal, clusters);
    assertEquals(1, roles.size());
    assertTrue(roles.contains(SECURITY_ADMIN));
  }

  @Test
  public void testBindResourceOwnerRole() throws IOException {
    apiClient.login(mdsUser, mdsPassword);
    apiClient.authenticate();
    apiClient.setKafkaClusterId(getKafkaClusterID());

    String principal = "User:fry" + System.currentTimeMillis();
    TopologyAclBinding binding =
        apiClient.bind(principal, RESOURCE_OWNER, "connect-configs", LITERAL);
    apiClient.bindRequest(binding);

    List<String> roles = apiClient.lookupRoles(principal);
    assertEquals(1, roles.size());
    assertTrue(roles.contains(RESOURCE_OWNER));
  }
}
