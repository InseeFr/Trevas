package fr.insee.vtl.engine.utils;

import fr.insee.vtl.model.Structured;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * KeyExtractor transform DataPoints to Map.
 *
 * <p>It is useful to compare datapoint based on a set of columns.
 */
public class KeyExtractor implements Function<Structured.DataPoint, Map<String, Object>> {

  private final Set<String> columns;

  public KeyExtractor(Collection<String> columns) {
    this.columns = new HashSet<>(columns);
  }

  @Override
  public Map<String, Object> apply(Structured.DataPoint objects) {
    List<Map.Entry<String, Object>> entries = new ArrayList<>(objects.size());
    for (String column : columns) {
      entries.add(Map.entry(column, objects.get(column)));
    }
    return Map.ofEntries(entries.toArray(Map.Entry[]::new));
  }
}
