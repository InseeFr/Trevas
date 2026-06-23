package fr.insee.vtl.engine.semantics.validation;

import static fr.insee.vtl.engine.utils.DefaultMeasureNames.BOOL_VAR;
import static fr.insee.vtl.model.Dataset.Component;
import static fr.insee.vtl.model.Dataset.Role.IDENTIFIER;
import static fr.insee.vtl.model.Dataset.Role.MEASURE;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.DataPointRule;
import fr.insee.vtl.model.DataPointRuleset;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.HierarchicalRule;
import fr.insee.vtl.model.HierarchicalRuleset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.ValidationOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * VTL validation orchestration ({@code validate_datapoint}, {@code check_datapoint}, {@code
 * check_hierarchy}) on top of mechanical {@link ProcessingEngine} operations.
 */
public final class ValidationExecutor {

  private static final String RULE_ID = "ruleid";
  private static final String ERROR_CODE = "errorcode";
  private static final String ERROR_LEVEL = "errorlevel";
  private static final String IMBALANCE = "imbalance";

  private ValidationExecutor() {}

  public static DatasetExpression validateDataPointRuleset(
      ProcessingEngine engine,
      DataPointRuleset dpr,
      DatasetExpression ds,
      String operandText,
      String rulesetName,
      String output,
      Positioned pos) {

    List<String> valuedomains = new ArrayList<>();
    Map<String, ResolvableExpression> colsToAdd = new HashMap<>();

    for (String vd : dpr.getValuedomains()) {
      List<String> vars =
          ds.getDataStructure().getByValuedomain(vd).stream()
              .map(Structured.Component::getName)
              .toList();
      if (vars.isEmpty()) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "Valuedomain " + vd + " not used in " + operandText + " components", pos));
      }
      if (vars.size() > 1) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "Valuedomain "
                    + vd
                    + " is used by "
                    + vars.size()
                    + " components in "
                    + operandText,
                pos));
      }
      dpr.setVariables(
          Stream.concat(dpr.getVariables().stream(), vars.stream()).collect(Collectors.toList()));

      valuedomains.add(vd);
      Class targetClass = ds.getDataStructure().get(vars.get(0)).getType();
      String varName = vars.get(0);
      colsToAdd.put(
          vd,
          ResolvableExpression.withType(targetClass)
              .withPosition(pos)
              .using(
                  c -> {
                    Map<String, Object> mapContext = (Map<String, Object>) c;
                    return mapContext.get(varName);
                  }));
    }

    ds = engine.executeCalc(ds, colsToAdd, Map.of(), Map.of());

    Structured.DataStructure dataStructure = ds.getDataStructure();
    for (String variable : dpr.getVariables()) {
      if (!dataStructure.containsKey(variable)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "Variable " + variable + " not contained in " + operandText, pos));
      }
    }
    for (String alias : dpr.getAlias().values()) {
      if (dataStructure.containsKey(alias)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "Alias "
                    + alias
                    + " from "
                    + rulesetName
                    + " ruleset already defined in "
                    + operandText,
                pos));
      }
    }

    return executeDataPointRuleset(engine, dpr, ds, output, pos, valuedomains);
  }

  public static DatasetExpression validateSimple(
      ProcessingEngine engine,
      DatasetExpression dsExpression,
      ResolvableExpression errorCodeExpr,
      ResolvableExpression errorLevelExpr,
      DatasetExpression imbalanceExpression,
      String output,
      Positioned pos) {

    List<Structured.Component> exprMeasures =
        dsExpression.getDataStructure().values().stream()
            .filter(Structured.Component::isMeasure)
            .toList();
    if (exprMeasures.size() != 1) {
      throw new VtlRuntimeException(
          new InvalidArgumentException("Check operand dataset contains several measures", pos));
    }
    if (exprMeasures.get(0).getType() != Boolean.class) {
      throw new VtlRuntimeException(
          new InvalidArgumentException("Check operand dataset measure has to be boolean", pos));
    }
    if (imbalanceExpression != null) {
      List<Structured.Component> imbalanceMeasures =
          imbalanceExpression.getDataStructure().values().stream()
              .filter(Structured.Component::isMeasure)
              .toList();
      if (imbalanceMeasures.size() != 1) {
        throw new VtlRuntimeException(
            new InvalidArgumentException("Check imbalance dataset contains several measures", pos));
      }
      List<Class<?>> supportedClasses = Arrays.asList(Double.class, Long.class);
      if (!supportedClasses.contains(imbalanceMeasures.get(0).getType())) {
        throw new VtlRuntimeException(
            new InvalidArgumentException("Check imbalance dataset measure has to be numeric", pos));
      }
    }

    return executeSimpleCheck(
        engine, dsExpression, errorCodeExpr, errorLevelExpr, imbalanceExpression, output, pos);
  }

  public static DatasetExpression validateHierarchical(
      ProcessingEngine engine,
      DatasetExpression dsExpression,
      HierarchicalRuleset hr,
      String datasetName,
      String componentId,
      String validationMode,
      String inputMode,
      String validationOutput,
      Positioned pos) {

    Structured.DataStructure dataStructure = dsExpression.getDataStructure();
    List<Structured.Component> measures = dataStructure.getMeasures();
    if (measures.size() != 1) {
      throw new VtlRuntimeException(
          new InvalidArgumentException("Dataset " + datasetName + " is not monomeasure", pos));
    }
    List<Class<?>> supportedClasses = Arrays.asList(Double.class, Long.class);
    if (!supportedClasses.contains(measures.get(0).getType())) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "Dataset "
                  + datasetName
                  + " measure "
                  + measures.get(0).getName()
                  + " has to have number type",
              pos));
    }
    if (!dataStructure.containsKey(componentId)) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "ComponentID " + componentId + " not contained in dataset " + datasetName, pos));
    }

    return executeHierarchicalCheck(
        engine, dsExpression, hr, componentId, validationMode, inputMode, validationOutput, pos);
  }

  public static DatasetExpression executeDataPointRuleset(
      ProcessingEngine engine,
      DataPointRuleset dpr,
      DatasetExpression dataset,
      String output,
      Positioned pos,
      List<String> toDrop) {

    DatasetExpression renamed = engine.executeRename(dataset, dpr.getAlias());
    Structured.DataStructure dataStructure = renamed.getDataStructure();
    Class errorCodeType = dpr.getErrorCodeType();
    Class errorLevelType = dpr.getErrorLevelType();
    Map<String, Dataset.Role> roles = dataPointRuleRoles();

    List<DatasetExpression> ruleDatasets =
        dpr.getRules().stream()
            .map(
                rule ->
                    engine.executeCalc(
                        renamed,
                        dataPointRuleExpressions(
                            rule, dataStructure, errorCodeType, errorLevelType, pos),
                        roles,
                        Map.of()))
            .toList();

    DatasetExpression united = engine.executeUnion(ruleDatasets, List.of());
    DatasetExpression inverted = engine.executeRename(united, invertMap(dpr.getAlias()));
    List<String> toKeep =
        inverted.getColumnNames().stream().filter(name -> !toDrop.contains(name)).toList();
    DatasetExpression cleaned = engine.executeProject(inverted, toKeep);

    if (output == null || output.equals(ValidationOutput.INVALID.value)) {
      return filterInvalidAndDropBoolVar(engine, cleaned, pos);
    }
    return cleaned;
  }

  public static DatasetExpression executeSimpleCheck(
      ProcessingEngine engine,
      DatasetExpression dsExpr,
      ResolvableExpression errorCodeExpr,
      ResolvableExpression errorLevelExpr,
      DatasetExpression imbalanceExpr,
      String output,
      Positioned pos) {

    String imbalanceMeasureName =
        imbalanceExpr.getDataStructure().values().stream()
            .filter(Component::isMeasure)
            .map(Component::getName)
            .collect(Collectors.toList())
            .get(0);

    DatasetExpression imbalanceRenamed =
        engine.executeRename(imbalanceExpr, Map.of(imbalanceMeasureName, IMBALANCE));

    List<Structured.Component> joinKeys =
        dsExpr.getDataStructure().values().stream()
            .filter(Component::isIdentifier)
            .collect(Collectors.toList());

    DatasetExpression joined =
        engine.executeLeftJoin(
            Map.of("dsExpr", dsExpr, "imbalanceExpr", imbalanceRenamed), joinKeys);

    DatasetExpression calculated =
        engine.executeCalc(
            joined,
            simpleCheckExpressions(errorCodeExpr, errorLevelExpr, pos),
            simpleCheckRoles(),
            Map.of());

    if (output == null || output.equals(ValidationOutput.ALL.value)) {
      return calculated;
    }
    return filterInvalidAndDropBoolVar(engine, calculated, pos);
  }

  public static DatasetExpression executeHierarchicalCheck(
      ProcessingEngine engine,
      DatasetExpression dsExpr,
      HierarchicalRuleset hr,
      String componentId,
      String validationMode,
      String inputMode,
      String validationOutput,
      Positioned pos) {

    if (inputMode != null && inputMode.equals("dataset_priority")) {
      throw new UnsupportedOperationException(
          "dataset_priority input mode is not supported in check_hierarchy");
    }

    Structured.Component measure = dsExpr.getDataStructure().getMeasures().get(0);
    Class<?> measureType = measure.getType();
    HierarchicalValidationRuntime runtime = requireHierarchicalRuntime(engine);
    Map<String, Object> bindings = runtime.columnBindings(dsExpr, componentId, hr.getVariable());
    Map<String, Dataset.Role> roles = hierarchicalRuleRoles();
    Class errorCodeType = hr.getErrorCodeType();
    Class errorLevelType = hr.getErrorLevelType();

    List<DatasetExpression> ruleDatasets = new ArrayList<>();
    for (HierarchicalRule rule : hr.getRules()) {
      DatasetExpression ruleDataset =
          hierarchicalRuleDataset(
              engine,
              runtime,
              dsExpr,
              rule,
              componentId,
              bindings,
              validationMode,
              measureType,
              errorCodeType,
              errorLevelType,
              roles,
              pos);
      if (ruleDataset != null) {
        ruleDatasets.add(ruleDataset);
      }
    }

    DatasetExpression result;
    if (ruleDatasets.isEmpty()) {
      Map<String, Dataset.Role> emptyRoles = new HashMap<>(hierarchicalRuleRoles());
      emptyRoles.put(measure.getName(), measure.getRole());
      emptyRoles.put(componentId, dsExpr.getDataStructure().get(componentId).getRole());
      result =
          DatasetExpression.of(
              new InMemoryDataset(
                  List.of(),
                  Map.of(
                      measure.getName(),
                      measureType,
                      RULE_ID,
                      String.class,
                      componentId,
                      String.class,
                      BOOL_VAR,
                      Boolean.class,
                      IMBALANCE,
                      Double.class,
                      ERROR_LEVEL,
                      errorLevelType,
                      ERROR_CODE,
                      errorCodeType),
                  emptyRoles),
              pos);
    } else {
      result = engine.executeUnion(ruleDatasets, List.of());
    }

    if (validationOutput == null || validationOutput.equals(ValidationOutput.INVALID.value)) {
      return filterInvalidAndDropBoolVar(engine, result, pos);
    }
    if (validationOutput.equals(ValidationOutput.ALL.value)) {
      return dropColumn(engine, result, measure.getName());
    }
    return result;
  }

  private static DatasetExpression hierarchicalRuleDataset(
      ProcessingEngine engine,
      HierarchicalValidationRuntime runtime,
      DatasetExpression dsExpr,
      HierarchicalRule rule,
      String componentId,
      Map<String, Object> bindings,
      String validationMode,
      Class<?> measureType,
      Class errorCodeType,
      Class errorLevelType,
      Map<String, Dataset.Role> roles,
      Positioned pos) {

    String filterText = componentId + " = \"" + rule.getValueDomainValue() + "\"";
    DatasetExpression filtered = runtime.filterKeepingSchema(dsExpr, filterText);

    List<String> codeItems = rule.getCodeItems();
    Map<String, Object> ruleBindings = extractRuleBindings(bindings, codeItems);
    if (Boolean.FALSE.equals(shouldProduceOutputLine(codeItems, ruleBindings, validationMode))) {
      return null;
    }
    ruleBindings = buildBindingsWithDefault(ruleBindings, codeItems, validationMode, measureType);

    Boolean expression = resolveBoolean(rule.getExpression(), ruleBindings);
    Double left = resolveDouble(rule.getLeftExpression(), ruleBindings);
    Double right = resolveDouble(rule.getRightExpression(), ruleBindings);

    return engine.executeCalc(
        filtered,
        hierarchicalRuleExpressions(
            rule,
            componentId,
            expression,
            measureType,
            left,
            right,
            errorCodeType,
            errorLevelType,
            pos),
        roles,
        Map.of());
  }

  private static HierarchicalValidationRuntime requireHierarchicalRuntime(ProcessingEngine engine) {
    if (engine instanceof HierarchicalValidationRuntime runtime) {
      return runtime;
    }
    throw new UnsupportedOperationException("check_hierarchy is not supported by this engine");
  }

  private static Map<String, ResolvableExpression> dataPointRuleExpressions(
      DataPointRule rule,
      Structured.DataStructure dataStructure,
      Class errorCodeType,
      Class errorLevelType,
      Positioned pos) {

    String ruleName = rule.getName();
    ResolvableExpression ruleId =
        ResolvableExpression.withType(String.class).withPosition(pos).using(c -> ruleName);
    ResolvableExpression antecedent = rule.getBuildAntecedentExpression(dataStructure);
    ResolvableExpression consequent = rule.getBuildConsequentExpression(dataStructure);
    ResolvableExpression errorCodeExpr = rule.getErrorCodeExpression();
    ResolvableExpression errorLevelExpr = rule.getErrorLevelExpression();

    ResolvableExpression errorCode =
        ResolvableExpression.withType(errorCodeType)
            .withPosition(pos)
            .using(
                context -> {
                  if (errorCodeExpr == null) return null;
                  Map<String, Object> ctx = (Map<String, Object>) context;
                  Object code = errorCodeExpr.resolve(ctx);
                  if (code == null) return null;
                  Boolean a = (Boolean) antecedent.resolve(ctx);
                  Boolean c = (Boolean) consequent.resolve(ctx);
                  return Boolean.TRUE.equals(a) && Boolean.FALSE.equals(c)
                      ? errorCodeType.cast(code)
                      : null;
                });

    ResolvableExpression errorLevel =
        ResolvableExpression.withType(errorLevelType)
            .withPosition(pos)
            .using(
                context -> {
                  if (errorLevelExpr == null) return null;
                  Map<String, Object> ctx = (Map<String, Object>) context;
                  Object level = errorLevelExpr.resolve(ctx);
                  if (level == null) return null;
                  Boolean a = (Boolean) antecedent.resolve(ctx);
                  Boolean c = (Boolean) consequent.resolve(ctx);
                  return Boolean.TRUE.equals(a) && Boolean.FALSE.equals(c)
                      ? errorLevelType.cast(level)
                      : null;
                });

    ResolvableExpression boolVar =
        ResolvableExpression.withType(Boolean.class)
            .withPosition(pos)
            .using(
                context -> {
                  Boolean a = (Boolean) antecedent.resolve(context);
                  Boolean c = (Boolean) consequent.resolve(context);
                  if (a == null) return c;
                  if (c == null) return a;
                  return !a || c;
                });

    return Map.of(
        RULE_ID, ruleId,
        BOOL_VAR, boolVar,
        ERROR_LEVEL, errorLevel,
        ERROR_CODE, errorCode);
  }

  private static Map<String, ResolvableExpression> simpleCheckExpressions(
      ResolvableExpression errorCodeExpr, ResolvableExpression errorLevelExpr, Positioned pos) {

    Class errorCodeType = errorCodeExpr == null ? String.class : errorCodeExpr.getType();
    ResolvableExpression errorCode =
        ResolvableExpression.withType(errorCodeType)
            .withPosition(pos)
            .using(
                context -> {
                  if (errorCodeExpr == null) return null;
                  Map<String, Object> ctx = (Map<String, Object>) context;
                  Boolean valid = (Boolean) ctx.get(BOOL_VAR);
                  return Boolean.TRUE.equals(valid)
                      ? null
                      : errorCodeType.cast(errorCodeExpr.resolve(ctx));
                });

    Class errorLevelType = errorLevelExpr == null ? String.class : errorLevelExpr.getType();
    ResolvableExpression errorLevel =
        ResolvableExpression.withType(errorLevelType)
            .withPosition(pos)
            .using(
                context -> {
                  if (errorLevelExpr == null) return null;
                  Map<String, Object> ctx = (Map<String, Object>) context;
                  Boolean valid = (Boolean) ctx.get(BOOL_VAR);
                  return Boolean.TRUE.equals(valid)
                      ? null
                      : errorLevelType.cast(errorLevelExpr.resolve(ctx));
                });

    return Map.of(ERROR_LEVEL, errorLevel, ERROR_CODE, errorCode);
  }

  private static Map<String, ResolvableExpression> hierarchicalRuleExpressions(
      HierarchicalRule rule,
      String componentId,
      Boolean expression,
      Class measureType,
      Double left,
      Double right,
      Class errorCodeType,
      Class errorLevelType,
      Positioned pos) {

    String ruleName = rule.getName();
    String valueDomain = rule.getValueDomainValue();
    ResolvableExpression errorCodeExpr = rule.getErrorCodeExpression();
    ResolvableExpression errorLevelExpr = rule.getErrorLevelExpression();

    ResolvableExpression errorCode =
        ResolvableExpression.withType(errorCodeType)
            .withPosition(pos)
            .using(
                context -> {
                  if (errorCodeExpr == null || expression == null) return null;
                  Map<String, Object> ctx = (Map<String, Object>) context;
                  Object code = errorCodeExpr.resolve(ctx);
                  return code != null && expression.equals(Boolean.FALSE)
                      ? errorCodeType.cast(code)
                      : null;
                });

    ResolvableExpression errorLevel =
        ResolvableExpression.withType(errorLevelType)
            .withPosition(pos)
            .using(
                context -> {
                  if (errorLevelExpr == null || expression == null) return null;
                  Map<String, Object> ctx = (Map<String, Object>) context;
                  Object level = errorLevelExpr.resolve(ctx);
                  return level != null && expression.equals(Boolean.FALSE)
                      ? errorLevelType.cast(level)
                      : null;
                });

    ResolvableExpression imbalance =
        ResolvableExpression.withType(measureType)
            .withPosition(pos)
            .using(
                context -> {
                  if (left == null || right == null) return null;
                  if (measureType.isAssignableFrom(Long.class)) {
                    return measureType.cast(left.longValue() - right.longValue());
                  }
                  return measureType.cast(left - right);
                });

    Map<String, ResolvableExpression> expressions = new HashMap<>();
    expressions.put(
        RULE_ID,
        ResolvableExpression.withType(String.class).withPosition(pos).using(c -> ruleName));
    expressions.put(
        componentId,
        ResolvableExpression.withType(String.class).withPosition(pos).using(c -> valueDomain));
    expressions.put(
        BOOL_VAR,
        ResolvableExpression.withType(Boolean.class).withPosition(pos).using(c -> expression));
    expressions.put(IMBALANCE, imbalance);
    expressions.put(ERROR_LEVEL, errorLevel);
    expressions.put(ERROR_CODE, errorCode);
    return expressions;
  }

  private static Map<String, Dataset.Role> dataPointRuleRoles() {
    return Map.of(
        RULE_ID, IDENTIFIER,
        BOOL_VAR, MEASURE,
        ERROR_LEVEL, MEASURE,
        ERROR_CODE, MEASURE);
  }

  private static Map<String, Dataset.Role> simpleCheckRoles() {
    return Map.of(ERROR_LEVEL, MEASURE, ERROR_CODE, MEASURE);
  }

  private static Map<String, Dataset.Role> hierarchicalRuleRoles() {
    Map<String, Dataset.Role> roles = new HashMap<>();
    roles.put(RULE_ID, IDENTIFIER);
    roles.put(BOOL_VAR, MEASURE);
    roles.put(IMBALANCE, MEASURE);
    roles.put(ERROR_LEVEL, MEASURE);
    roles.put(ERROR_CODE, MEASURE);
    return roles;
  }

  private static DatasetExpression filterInvalidAndDropBoolVar(
      ProcessingEngine engine, DatasetExpression dataset, Positioned pos) {
    ResolvableExpression defaultExpression =
        ResolvableExpression.withType(Boolean.class).withPosition(pos).using(c -> null);
    DatasetExpression filtered =
        engine.executeFilter(dataset, defaultExpression, BOOL_VAR + " = false");
    return dropColumn(engine, filtered, BOOL_VAR);
  }

  private static DatasetExpression dropColumn(
      ProcessingEngine engine, DatasetExpression dataset, String column) {
    List<String> toKeep =
        dataset.getColumnNames().stream().filter(name -> !name.equals(column)).toList();
    return engine.executeProject(dataset, toKeep);
  }

  private static Map<String, Object> extractRuleBindings(
      Map<String, Object> bindings, List<String> items) {
    Map<String, Object> ruleBindings = new HashMap<>();
    items.forEach(
        k -> {
          if (bindings.containsKey(k)) {
            ruleBindings.put(k, bindings.get(k));
          }
        });
    return ruleBindings;
  }

  private static Boolean shouldProduceOutputLine(
      List<String> codeItems, Map<String, Object> ruleBindings, String validationMode) {
    if (validationMode == null || validationMode.equals("non_null")) {
      return codeItems.size() == ruleBindings.size()
          && ruleBindings.values().stream().noneMatch(Objects::isNull);
    }
    if (validationMode.equals("non_zero")) {
      return ruleBindings.values().stream()
          .anyMatch(
              r -> {
                if (r == null) return true;
                double d = r instanceof Long l ? l.doubleValue() : (Double) r;
                return d != 0D;
              });
    }
    if (validationMode.equals("partial_null") || validationMode.equals("partial_zero")) {
      return ruleBindings.values().stream().anyMatch(Objects::nonNull);
    }
    if (validationMode.equals("always_null") || validationMode.equals("always_zero")) {
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }

  private static Map<String, Object> buildBindingsWithDefault(
      Map<String, Object> bindings,
      List<String> ruleItems,
      String validationMode,
      Class<?> measureType) {
    Map<String, Object> result = new HashMap<>();
    ruleItems.forEach(
        item -> {
          if (bindings.containsKey(item)) {
            result.put(item, bindings.get(item));
          } else if (List.of("non_zero", "partial_zero", "always_zero").contains(validationMode)) {
            result.put(item, measureType.isAssignableFrom(Long.class) ? 0L : 0D);
          } else if (List.of("partial_null", "always_null").contains(validationMode)) {
            result.put(item, null);
          }
        });
    return result;
  }

  private static Boolean resolveBoolean(
      ResolvableExpression expression, Map<String, Object> bindings) {
    try {
      return (Boolean) expression.resolve(bindings);
    } catch (Exception e) {
      return null;
    }
  }

  private static Double resolveDouble(
      ResolvableExpression expression, Map<String, Object> bindings) {
    try {
      return (Double) expression.resolve(bindings);
    } catch (Exception e) {
      return null;
    }
  }

  private static <V, K> Map<V, K> invertMap(Map<K, V> map) {
    return map.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
  }
}
