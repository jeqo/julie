package kafka.ops.topology.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import kafka.ops.topology.utils.JSON;

public abstract class BaseAction implements Action {

  protected abstract Map<String, Object> props();

  @Override
  public String toString() {
    try {
      return JSON.asPrettyString(props());
    } catch (JsonProcessingException e) {
      return "";
    }
  }
}