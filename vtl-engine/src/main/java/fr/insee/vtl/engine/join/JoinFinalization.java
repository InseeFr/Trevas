package fr.insee.vtl.engine.join;

import static fr.insee.vtl.model.Dataset.Role;

import fr.insee.vtl.engine.attribute.AttributePropagationAlgorithm;
import fr.insee.vtl.engine.attribute.ViralColumnMergePlan;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Post-join alias stripping and viral merge via {@link ProcessingEngine} primitives only. */
public final class JoinFinalization {

  private static final Positioned POS =
      () -> new Positioned.Position("join-finalization", 0, 0, 0, 0);

  private JoinFinalization() {}

  public static DatasetExpression apply(
      ProcessingEngine engine, DatasetExpression joined, List<String> outputColumnNames) {
    DataStructure source = joined.getDataStructure();
    Map<String, ResolvableExpression> calcs = new LinkedHashMap<>();
    Map<String, Role> roles = new LinkedHashMap<>();
    Set<String> computed = new HashSet<>();

    for (String bareName : outputColumnNames) {
      List<Component> viralSources = ViralColumnMergePlan.viralSources(source, bareName);
      if (viralSources.size() > 1) {
        calcs.put(bareName, mergeExpr(viralSources));
        roles.put(bareName, viralSources.get(0).getRole());
        computed.add(bareName);
        continue;
      }
      String physical = JoinProjection.resolveSourceColumn(source, bareName);
      Component component = source.get(physical);
      if (component == null) {
        continue;
      }
      if (!physical.equals(bareName) || needsOverwrite(source, bareName, physical)) {
        calcs.put(bareName, new ComponentExpression(component, POS));
        roles.put(bareName, component.getRole());
        computed.add(bareName);
      }
    }

    DatasetExpression current =
        calcs.isEmpty() ? joined : engine.executeCalc(joined, calcs, roles, Map.of());

    Map<String, String> renames = new LinkedHashMap<>();
    for (String bareName : outputColumnNames) {
      if (computed.contains(bareName)) {
        continue;
      }
      String physical = JoinProjection.resolveSourceColumn(source, bareName);
      if (!physical.equals(bareName)) {
        renames.put(physical, bareName);
      }
    }
    if (!renames.isEmpty()) {
      current = engine.executeRename(current, renames);
    }
    return engine.executeProject(current, outputColumnNames);
  }

  private static boolean needsOverwrite(DataStructure source, String bareName, String physical) {
    return source.containsKey(bareName) && !physical.equals(bareName);
  }

  private static ResolvableExpression mergeExpr(List<Component> sources) {
    Class<?> type = sources.get(0).getType();
    return ResolvableExpression.withType(type)
        .withPosition(POS)
        .using(
            ctx -> {
              Object merged = null;
              for (Component component : sources) {
                Object value = ctx.get(component.getName());
                merged =
                    merged == null
                        ? value
                        : AttributePropagationAlgorithm.propagateBinaryValue(merged, value, type);
              }
              return cast(type, merged);
            });
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Class<?> type, Object value) {
    return (T) type.cast(value);
  }
}
