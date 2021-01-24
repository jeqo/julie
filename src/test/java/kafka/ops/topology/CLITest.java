package kafka.ops.topology;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class CLITest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  private BuilderCli cli;

  @Before
  public void setup() {
    cli = Mockito.spy(new BuilderCli());
  }

  @Test
  public void testParamPassing() throws Exception {
    String[] args =
        new String[] {
          "--brokers", "localhost:9092",
          "--topology", "descriptor.yaml",
          "--clientConfig", "topology-builder-sasl-plain.properties"
        };

    doNothing().when(cli).processTopology(eq("descriptor.yaml"), eq("default"), anyMap());

    Map<String, String> config = new HashMap<>();
    config.put(BuilderCli.BROKERS_OPTION, "localhost:9092");
    config.put(BuilderCli.ALLOW_DELETE_OPTION, "false");
    config.put(BuilderCli.DRY_RUN_OPTION, "false");
    config.put(BuilderCli.QUIET_OPTION, "false");
    config.put(BuilderCli.ADMIN_CLIENT_CONFIG_OPTION, "topology-builder-sasl-plain.properties");
    cli.run(args);

    verify(cli, times(1)).processTopology(eq("descriptor.yaml"), eq("default"), eq(config));
  }

  @Test
  public void testDryRun() throws Exception {
    String[] args =
        new String[] {
          "--brokers", "localhost:9092",
          "--topology", "descriptor.yaml",
          "--clientConfig", "topology-builder-sasl-plain.properties",
          "--dryRun"
        };

    doNothing().when(cli).processTopology(eq("descriptor.yaml"), eq("default"), anyMap());

    Map<String, String> config = new HashMap<>();
    config.put(BuilderCli.BROKERS_OPTION, "localhost:9092");
    config.put(BuilderCli.ALLOW_DELETE_OPTION, "false");
    config.put(BuilderCli.DRY_RUN_OPTION, "true");
    config.put(BuilderCli.QUIET_OPTION, "false");
    config.put(BuilderCli.ADMIN_CLIENT_CONFIG_OPTION, "topology-builder-sasl-plain.properties");
    cli.run(args);

    verify(cli, times(1)).processTopology(eq("descriptor.yaml"), eq("default"), eq(config));
  }
}