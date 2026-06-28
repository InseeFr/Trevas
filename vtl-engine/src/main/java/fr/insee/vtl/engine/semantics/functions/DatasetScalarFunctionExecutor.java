package fr.insee.vtl.engine.semantics.functions;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.expressions.FunctionExpression;
import fr.insee.vtl.engine.semantics.attribute.AttributePropagation;
import fr.insee.vtl.engine.semantics.attribute.UnaryAttributePropagation;
import fr.insee.vtl.engine.semantics.attribute.ViralReattach;
import fr.insee.vtl.engine.semantics.join.JoinExecutor;
import fr.insee.vtl.engine.utils.DefaultMeasureNames;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Scalar VTL functions applied row-wise on mono-measure dataset operands. */
public final class DatasetScalarFunctionExecutor {

  private DatasetScalarFunctionExecutor() {}

  public static ResolvableExpression invoke(
      VtlScriptEngine engine,
      String funcName,
      List<ResolvableExpression> parameters,
      Positioned position)
      throws NoSuchMethodException, VtlScriptException {
    List<DatasetExpression> multiMeasureOperands =
        parameters.stream()
            .filter(e -> e instanceof DatasetExpression de && !de.isMonoMeasure())
            .map(DatasetExpression.class::cast)
            .toList();
    if (multiMeasureOperands.size() > 2) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "too many no mono-measure datasets (" + multiMeasureOperands.size() + ")", position));
    }

    List<Class> parameterTypes =
        parameters.stream().map(ResolvableExpression::getType).collect(Collectors.toList());
    var method = engine.findGlobalMethod(funcName, parameterTypes);
    if (parameters.stream().noneMatch(DatasetExpression.class::isInstance) || method != null) {
      if (method == null) {
        method = engine.findMethod(funcName, parameterTypes);
      }
      return new FunctionExpression(method, parameters, position);
    }
    if (multiMeasureOperands.isEmpty()) {
      return invokeOnMonoMeasureOperands(engine, funcName, parameters, position, true);
    }
    return invokePerMeasureWithViralReattach(
        engine,
        funcName,
        parameters,
        position,
        multiMeasureOperands.get(0),
        multiMeasureOperands.get(0).getDataStructure().getMeasures());
  }

  public static DatasetExpression invokeOnMonoMeasureOperands(
      VtlScriptEngine engine,
      String funcName,
      List<ResolvableExpression> parameters,
      Positioned position,
      boolean monoMeasureOperands)
      throws NoSuchMethodException, VtlScriptException {
    ProcessingEngine proc = engine.getProcessingEngine();

    Map<String, ResolvableExpression> monoExprs = new LinkedHashMap<>();
    Map<DatasetExpression, String> operandAliases = new LinkedHashMap<>();
    Map<String, DatasetExpression> dsExprs = new LinkedHashMap<>();
    Set<String> measureNames = new HashSet<>();
    List<DatasetExpression> operandDatasets = new ArrayList<>();
    for (ResolvableExpression parameter : parameters) {
      if (parameter instanceof DatasetExpression ds) {
        operandDatasets.add(ds);
      }
    }
    int argIndex = 0;
    for (DatasetExpression ds : operandDatasets) {
      if (Boolean.FALSE.equals(ds.isMonoMeasure())) {
        throw new VtlRuntimeException(
            new InvalidArgumentException("mono-measure dataset expected", ds));
      }
      String operandAlias = "arg" + argIndex++;
      operandAliases.put(ds, operandAlias);
      var measure = ds.getMeasures().get(0);
      String measureName = measure.getName();
      measureNames.add(measureName);
      ds =
          proc.executeProject(
              ds,
              UnaryAttributePropagation.columnsForMonoMeasureOperation(
                  ds.getDataStructure(), measureName));
      Map<String, String> joinRenames = new LinkedHashMap<>();
      joinRenames.put(measureName, operandAlias);
      for (String viral : AttributePropagation.viralAttributeNames(ds.getDataStructure())) {
        joinRenames.put(viral, operandAlias + "#" + viral);
      }
      ds = proc.executeRename(ds, joinRenames);
      var renamedComponent =
          new Structured.Component(
              operandAlias, measure.getType(), measure.getRole(), measure.getNullable());
      monoExprs.put(operandAlias, new ComponentExpression(renamedComponent, ds));
      dsExprs.put(operandAlias, ds);
    }
    if (measureNames.size() != 1) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "Variables in the mono-measure datasets are not named the same: "
                  + measureNames
                  + " found",
              position));
    }
    DatasetExpression ds = JoinExecutor.innerJoinInferringKeys(proc, dsExprs);

    var normalizedParams =
        parameters.stream()
            .map(
                e -> {
                  if (e instanceof DatasetExpression operand) {
                    String alias = operandAliases.get(operand);
                    if (alias != null) {
                      return monoExprs.get(alias);
                    }
                  }
                  return e;
                })
            .collect(Collectors.toList());

    List<Class> parametersTypes =
        normalizedParams.stream().map(TypedExpression::getType).collect(Collectors.toList());
    var method = engine.findMethod(funcName, parametersTypes);
    var funcExpr = new FunctionExpression(method, normalizedParams, position);
    Class<?> resultType = funcExpr.getType();
    Class<?> operandMeasureType =
        DefaultMeasureNames.operandMeasureType(parameters, measureNames, resultType);
    ds =
        proc.executeCalc(
            ds, Map.of("result", funcExpr), Map.of("result", Dataset.Role.MEASURE), Map.of());
    ds =
        proc.executeProject(
            ds,
            UnaryAttributePropagation.columnsForUnaryOutput(
                ds.getDataStructure(), List.of("result")));
    String outputMeasureName =
        DefaultMeasureNames.resolveOutputMeasureName(
            measureNames.iterator().next(), operandMeasureType, resultType, monoMeasureOperands);
    ds = proc.executeRename(ds, Map.of("result", outputMeasureName));
    List<DatasetExpression> datasetOperands =
        parameters.stream()
            .filter(DatasetExpression.class::isInstance)
            .map(DatasetExpression.class::cast)
            .toList();
    if (!datasetOperands.isEmpty()) {
      Map<String, Class<?>> outputMeasures = Map.of(outputMeasureName, resultType);
      return ViralReattach.binary(proc, datasetOperands, ds, outputMeasures);
    }
    return ds;
  }

  public static DatasetExpression invokePerMeasureWithViralReattach(
      VtlScriptEngine engine,
      String funcName,
      List<ResolvableExpression> parameters,
      Positioned position,
      DatasetExpression viralSource,
      List<Structured.Component> measures)
      throws NoSuchMethodException, VtlScriptException {
    ProcessingEngine proc = engine.getProcessingEngine();
    Map<String, DatasetExpression> results = new LinkedHashMap<>();
    for (Structured.Component measure : measures) {
      List<ResolvableExpression> params =
          parameters.stream()
              .map(
                  p -> {
                    if (p instanceof DatasetExpression ds) {
                      return proc.executeProject(
                          ds,
                          UnaryAttributePropagation.columnsForMonoMeasureOperation(
                              ds.getDataStructure(), measure.getName()));
                    }
                    return p;
                  })
              .collect(Collectors.toList());
      results.put(
          measure.getName(),
          invokeOnMonoMeasureOperands(engine, funcName, params, position, false));
    }
    DatasetExpression joined = JoinExecutor.innerJoinInferringKeys(proc, results);
    Map<String, Class<?>> outputMeasures =
        joined.getDataStructure().getMeasures().stream()
            .collect(
                Collectors.toMap(Structured.Component::getName, Structured.Component::getType));
    return ViralReattach.unary(proc, viralSource, joined, outputMeasures);
  }
}
