package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.FunctionNotFoundException;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.CastExpression;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.engine.expressions.FunctionExpression;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.TypedExpression;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

/** <code>GenericFunctionsVisitor</code> is the base visitor for cast expressions. */
public class GenericFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private static final String result = "result";
  private final VtlScriptEngine engine;
  private final ExpressionVisitor exprVisitor;

  /**
   * Constructor taking an expression visitor.
   *
   * @param expressionVisitor The visitor for the enclosing expression.
   * @param engine The {@link VtlScriptEngine}.
   */
  public GenericFunctionsVisitor(ExpressionVisitor expressionVisitor, VtlScriptEngine engine) {
    this.engine = Objects.requireNonNull(engine);
    exprVisitor = Objects.requireNonNull(expressionVisitor);
  }

  /**
   * Method to map basic scalar types and classes.
   *
   * @param basicScalarType Basic scalar type.
   * @param basicScalarText Basic scalar text.
   */
  private static Class<?> getOutputClass(Integer basicScalarType, String basicScalarText) {
    return switch (basicScalarType) {
      case VtlParser.STRING -> String.class;
      case VtlParser.INTEGER -> Long.class;
      case VtlParser.NUMBER -> Double.class;
      case VtlParser.BOOLEAN -> Boolean.class;
      case VtlParser.DATE -> Instant.class;
      case VtlParser.DURATION -> PeriodDuration.class;
      case VtlParser.TIME_PERIOD -> Interval.class;
      default ->
          throw new UnsupportedOperationException(
              "basic scalar type " + basicScalarText + " unsupported");
    };
  }

  public List<DatasetExpression> splitToMonoMeasure(DatasetExpression dataset) {
    ProcessingEngine proc = engine.getProcessingEngine();
    List<Structured.Component> identifiers = dataset.getIdentifiers();
    return dataset.getMeasures().stream()
        .map(
            measure -> {
              List<String> idAndMeasure =
                  Stream.concat(identifiers.stream(), Stream.of(measure))
                      .map(Structured.Component::getName)
                      .collect(Collectors.toList());
              return proc.executeProject(dataset, idAndMeasure);
            })
        .collect(Collectors.toList());
  }

  public ResolvableExpression invokeFunction(
      String funcName, List<ResolvableExpression> parameters, Positioned position)
      throws VtlScriptException {
    try {
      List<DatasetExpression> noMonoDs =
          parameters.stream()
              .filter(e -> e instanceof DatasetExpression de && !(de.isMonoMeasure()))
              .map(ds -> (DatasetExpression) ds)
              .toList();
      if (noMonoDs.size() > 2) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "too many no mono-measure datasets (" + noMonoDs.size() + ")", position));
      }

      ProcessingEngine proc = engine.getProcessingEngine();

      // TODO: if (noMonoDs has not same shape) throw

      // Invoking a function only supports a combination of scalar types and mono-measure arrays. In
      // the special
      // case of bi-functions (a + b or f(a,b)) the two datasets must have the same identifiers and
      // measures.
      ResolvableExpression finalRes;
      List<Class> parameterTypes =
          parameters.stream().map(ResolvableExpression::getType).collect(Collectors.toList());
      // Only one parameter, and it's a dataset. We can invoke the function on each measure.
      // Or method found in global methods
      var method = engine.findGlobalMethod(funcName, parameterTypes);
      if (parameters.stream().noneMatch(DatasetExpression.class::isInstance) || method != null) {
        // Only scalar types. We can invoke the function directly.
        if (method == null) {
          method = engine.findMethod(funcName, parameterTypes);
        }
        return new FunctionExpression(method, parameters, position);
      } else if (noMonoDs.isEmpty()) {
        finalRes = invokeFunctionOnDataset(funcName, parameters, position);
      } else {
        List<Structured.Component> measures = noMonoDs.get(0).getDataStructure().getMeasures();
        Map<String, DatasetExpression> results = new HashMap<>();
        for (Structured.Component measure : measures) {
          List<ResolvableExpression> params =
              parameters.stream()
                  .map(
                      p -> {
                        if (p instanceof DatasetExpression ds) {
                          List<String> idAndMeasure =
                              Stream.concat(ds.getIdentifiers().stream(), Stream.of(measure))
                                  .map(Structured.Component::getName)
                                  .collect(Collectors.toList());
                          return proc.executeProject(ds, idAndMeasure);
                        } else return p;
                      })
                  .collect(Collectors.toList());
          results.put(measure.getName(), invokeFunctionOnDataset(funcName, params, position));
        }
        finalRes = proc.executeInnerJoin(results);
      }
      if (finalRes instanceof DatasetExpression expression) {
        List<Structured.Component> measures = expression.getMeasures();
        if (measures.size() == 1 && measures.get(0).getType().equals(Boolean.class)) {
          // TODO: refine with constraints matrix
          return proc.executeRename(expression, Map.of(measures.get(0).getName(), "bool_var"));
        }
      }
      return finalRes;
    } catch (NoSuchMethodException e) {
      throw new VtlRuntimeException(new FunctionNotFoundException(e.getMessage(), position));
    }
  }

  private DatasetExpression invokeFunctionOnDataset(
      String funcName, List<ResolvableExpression> parameters, Positioned position)
      throws NoSuchMethodException, VtlScriptException {
    ProcessingEngine proc = engine.getProcessingEngine();

    // Normalize all parameters to datasets first.
    // 1. Join all the datasets together and build a new expression map.
    Map<String, ResolvableExpression> monoExprs = new HashMap<>();
    Set<String> measureNames = new HashSet<>();
    var dsExprs =
        parameters.stream()
            .filter(DatasetExpression.class::isInstance)
            .map(e -> ((DatasetExpression) e))
            .map(
                ds -> {
                  if (Boolean.FALSE.equals(ds.isMonoMeasure())) {
                    throw new VtlRuntimeException(
                        new InvalidArgumentException("mono-measure dataset expected", ds));
                  }
                  var uniqueName = "arg" + ds.hashCode();
                  var measure = ds.getMeasures().get(0);
                  String measureName = measure.getName();
                  measureNames.add(measureName);
                  ds = proc.executeRename(ds, Map.of(measureName, uniqueName));
                  var renamedComponent =
                      new Structured.Component(
                          uniqueName, measure.getType(), measure.getRole(), measure.getNullable());
                  monoExprs.put(uniqueName, new ComponentExpression(renamedComponent, ds));
                  return ds;
                })
            .collect(Collectors.toMap(e -> "arg" + e.hashCode(), e -> e));
    if (measureNames.size() != 1) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "Variables in the mono-measure datasets are not named the same: "
                  + measureNames
                  + " found",
              position));
    }
    DatasetExpression ds = proc.executeInnerJoin(dsExprs);

    // Rebuild the function parameters. TODO: All component?
    var normalizedParams =
        parameters.stream()
            .map(e -> monoExprs.getOrDefault("arg" + e.hashCode(), e))
            .collect(Collectors.toList());

    // 3. Invoke the function.
    List<Class> parametersTypes =
        normalizedParams.stream().map(TypedExpression::getType).collect(Collectors.toList());
    var method = engine.findMethod(funcName, parametersTypes);
    var funcExrp = new FunctionExpression(method, normalizedParams, position);
    ds =
        proc.executeCalc(
            ds, Map.of(result, funcExrp), Map.of(result, Dataset.Role.MEASURE), Map.of());
    ds =
        proc.executeProject(
            ds,
            Stream.concat(
                    ds.getIdentifiers().stream().map(Structured.Component::getName),
                    Stream.of(result))
                .collect(Collectors.toList()));
    return proc.executeRename(ds, Map.of(result, measureNames.iterator().next()));
  }

  @Override
  public ResolvableExpression visitCallDataset(VtlParser.CallDatasetContext ctx) {
    // Strange name, this is the generic function syntax; fnName ( param, * ).
    try {
      List<ResolvableExpression> parameters =
          ctx.parameter().stream().map(exprVisitor::visit).collect(Collectors.toList());
      return invokeFunction(ctx.operatorID().getText(), parameters, fromContext(ctx));
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }

  /**
   * Visits expressions with cast operators.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the cast operation.
   */
  @Override
  public ResolvableExpression visitCastExprDataset(VtlParser.CastExprDatasetContext ctx) {
    ResolvableExpression expression = exprVisitor.visit(ctx.expr());
    TerminalNode maskNode = ctx.STRING_CONSTANT();
    // STRING_CONSTANT().getText return null or a string wrapped by quotes
    String mask =
        maskNode == null
            ? null
            : maskNode.getText().replace("\"", "").replace("YYYY", "yyyy").replace("DD", "dd");
    Token symbol = ((TerminalNode) ctx.basicScalarType().getChild(0)).getSymbol();
    Integer basicScalarType = symbol.getType();
    String basicScalarText = symbol.getText();

    Class<?> outputClass = getOutputClass(basicScalarType, basicScalarText);

    if (Object.class.equals(expression.getType())) {
      return ResolvableExpression.withType(outputClass)
          .withPosition(fromContext(ctx))
          .using(c -> null);
    }
    try {
      return new CastExpression(fromContext(ctx), expression, mask, outputClass);
    } catch (VtlScriptException e) {
      throw new VtlRuntimeException(e);
    }
  }
}
