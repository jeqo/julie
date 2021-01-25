package com.purbon.kafka.topology.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.purbon.kafka.topology.utils.Json;
import java.util.Map;

public abstract class BaseAction implements Action {

  protected abstract Map<String, Object> props();

  @Override
  public String toString() {
    try {
      return Json.asPrettyString(props());
    } catch (JsonProcessingException e) {
      return "";
    }
  }
}
