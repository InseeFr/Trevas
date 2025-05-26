package fr.insee.vtl.engine.visitors.expression;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.AnalyticsVisitor;
import fr.insee.vtl.engine.visitors.ClauseVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.ComparisonFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.DistanceFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.JoinFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.NumericFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.SetFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.StringFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.TimeFunctionsVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.ValidationFunctionsVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <code>ExpressionVisitor</code> is the base visitor for expressions. It essentially passes the
 * expressions to the more specialized visitors defined in the package.
 */
public class ExpressionVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private static final ConstantVisitor CONSTANT_VISITOR = new ConstantVisitor();
  private final VarIdVisitor varIdVisitor;
  private final BooleanVisitor booleanVisitor;
  private final ArithmeticVisitor arithmeticVisitor;
  private final ArithmeticExprOrConcatVisitor arithmeticExprOrConcatVisitor;
  private final UnaryVisitor unaryVisitor;
  private final ComparisonVisitor comparisonVisitor;
  private final ConditionalVisitor conditionalVisitor;
  private final StringFunctionsVisitor stringFunctionsVisitor;
  private final ComparisonFunctionsVisitor comparisonFunctionsVisitor;
  private final NumericFunctionsVisitor numericFunctionsVisitor;
  private final SetFunctionsVisitor setFunctionsVisitor;
  private final JoinFunctionsVisitor joinFunctionsVisitor;
  private final GenericFunctionsVisitor genericFunctionsVisitor;
  private final DistanceFunctionsVisitor distanceFunctionsVisitor;
  private final TimeFunctionsVisitor timeFunctionsVisitor;
  private final ValidationFunctionsVisitor validationFunctionsVisitor;
  private final ProcessingEngine processingEngine;
  private final VtlScriptEngine engine;

  /**
   * Constructor taking a scripting context and a processing engine.
   *
   * @param context The map representing the context.
   * @param processingEngine The processing engine.
   */
  // TODO: Use script context to get bindings
  public ExpressionVisitor(
      Map<String, Object> context, ProcessingEngine processingEngine, VtlScriptEngine engine) {
    Objects.requireNonNull(context);
    genericFunctionsVisitor = new GenericFunctionsVisitor(this, engine);
    varIdVisitor = new VarIdVisitor(context);
    booleanVisitor = new BooleanVisitor(this, genericFunctionsVisitor);
    arithmeticVisitor = new ArithmeticVisitor(this, genericFunctionsVisitor);
    arithmeticExprOrConcatVisitor =
        new ArithmeticExprOrConcatVisitor(this, genericFunctionsVisitor);
    unaryVisitor = new UnaryVisitor(this, genericFunctionsVisitor);
    comparisonVisitor = new ComparisonVisitor(this, genericFunctionsVisitor);
    conditionalVisitor = new ConditionalVisitor(this, genericFunctionsVisitor);
    stringFunctionsVisitor = new StringFunctionsVisitor(this, genericFunctionsVisitor);
    comparisonFunctionsVisitor = new ComparisonFunctionsVisitor(this, genericFunctionsVisitor);
    setFunctionsVisitor = new SetFunctionsVisitor(this, processingEngine);
    joinFunctionsVisitor = new JoinFunctionsVisitor(this, processingEngine);
    numericFunctionsVisitor = new NumericFunctionsVisitor(this, genericFunctionsVisitor);
    distanceFunctionsVisitor = new DistanceFunctionsVisitor(this, genericFunctionsVisitor);
    timeFunctionsVisitor =
        new TimeFunctionsVisitor(genericFunctionsVisitor, this, processingEngine);
    validationFunctionsVisitor = new ValidationFunctionsVisitor(this, processingEngine, engine);
    this.processingEngine = Objects.requireNonNull(processingEngine);
    this.engine = Objects.requireNonNull(engine);
  }

  /**
   * Visits constant expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the constant value with the expected
   *     type.
   * @see ConstantVisitor#visitConstant(VtlParser.ConstantContext)
   */
  @Override
  public ResolvableExpression visitConstant(VtlParser.ConstantContext ctx) {
    return CONSTANT_VISITOR.visit(ctx);
  }

  @Override
  public ResolvableExpression visitVarID(VtlParser.VarIDContext ctx) {
    return varIdVisitor.visit(ctx);
  }

  /**
   * Visits expressions with boolean operators.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the boolean operation.
   * @see BooleanVisitor#visitBooleanExpr(VtlParser.BooleanExprContext)
   */
  @Override
  public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
    return booleanVisitor.visit(ctx);
  }

  @Override
  public ResolvableExpression visitMembershipExpr(VtlParser.MembershipExprContext ctx) {
    try {
      ResolvableExpression ds = this.visit(ctx.expr());
      if (!(ds instanceof DatasetExpression)) {
        throw new InvalidTypeException(Dataset.class, ds.getType(), fromContext(ctx.expr()));
      }
      Structured.DataStructure structure = ((DatasetExpression) ds).getDataStructure();
      String componentName = ctx.simpleComponentId().getText();
      if (!structure.containsKey(componentName)) {
        throw new VtlScriptException(
            "column %s not found in %s".formatted(componentName, ctx.expr().getText()),
            fromContext(ctx));
      }

      ArrayList<String> components =
          structure.values().stream()
              .filter(Structured.Component::isIdentifier)
              .map(Structured.Component::getName)
              .collect(Collectors.toCollection(ArrayList::new));
      components.add(componentName);
      return this.engine.getProcessingEngine().executeProject((DatasetExpression) ds, components);
    } catch (VtlScriptException vse) {
      throw new VtlRuntimeException(vse);
    }
  }

  /**
   * Visits expressions with multiplication or division operators.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the multiplication or
   *     division operation.
   * @see ArithmeticVisitor#visitArithmeticExpr(VtlParser.ArithmeticExprContext)
   */
  @Override
  public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
    return arithmeticVisitor.visit(ctx);
  }

  /**
   * Visits expressions with plus, minus or concatenation operators.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the plus, minus or
   *     concatenation operation.
   * @see
   *     ArithmeticExprOrConcatVisitor#visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext)
   */
  @Override
  public ResolvableExpression visitArithmeticExprOrConcat(
      VtlParser.ArithmeticExprOrConcatContext ctx) {
    return arithmeticExprOrConcatVisitor.visit(ctx);
  }

  /**
   * Visits unary expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the unary operation.
   * @see UnaryVisitor#visitUnaryExpr(VtlParser.UnaryExprContext)
   */
  @Override
  public ResolvableExpression visitUnaryExpr(VtlParser.UnaryExprContext ctx) {
    return unaryVisitor.visit(ctx);
  }

  /**
   * Visits expressions between parentheses (just passes the expression down the tree).
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> corresponding to the expression between
   *     parentheses.
   */
  @Override
  public ResolvableExpression visitParenthesisExpr(VtlParser.ParenthesisExprContext ctx) {
    return visit(ctx.expr());
  }

  /**
   * Visits expressions with comparisons.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the boolean result of the comparison.
   * @see ComparisonVisitor#visitComparisonExpr(VtlParser.ComparisonExprContext)
   */
  @Override
  public ResolvableExpression visitComparisonExpr(VtlParser.ComparisonExprContext ctx) {
    return comparisonVisitor.visit(ctx);
  }

  /**
   * Visits 'element of' ('In' or 'Not in') expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the boolean result of the 'element of'
   *     expression.
   * @see ComparisonVisitor#visitInNotInExpr(VtlParser.InNotInExprContext)
   */
  @Override
  public ResolvableExpression visitInNotInExpr(VtlParser.InNotInExprContext ctx) {
    return comparisonVisitor.visit(ctx);
  }

  /**
   * Visits if-then-else expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the if or else clause resolution
   *     depending on the condition resolution.
   * @see ConditionalVisitor#visitIfExpr(VtlParser.IfExprContext)
   */
  @Override
  public ResolvableExpression visitIfExpr(VtlParser.IfExprContext ctx) {
    return conditionalVisitor.visit(ctx);
  }

  /**
   * Visits case expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the case resolution depending on the
   *     condition resolution.
   * @see ConditionalVisitor#visitIfExpr(VtlParser.IfExprContext)
   */
  @Override
  public ResolvableExpression visitCaseExpr(VtlParser.CaseExprContext ctx) {
    return conditionalVisitor.visit(ctx);
  }

  /**
   * Visits nvl expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the null value clause resolution.
   * @see ConditionalVisitor#visitIfExpr(VtlParser.IfExprContext)
   */
  @Override
  public ResolvableExpression visitNvlAtom(VtlParser.NvlAtomContext ctx) {
    return conditionalVisitor.visit(ctx);
  }

  /*
  Functions
   */

  /**
   * Visits expressions involving string functions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the string function.
   * @see StringFunctionsVisitor
   */
  @Override
  public ResolvableExpression visitStringFunctions(VtlParser.StringFunctionsContext ctx) {
    return stringFunctionsVisitor.visit(ctx.stringOperators());
  }

  /**
   * Visits expressions involving comparison functions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the comparison function.
   * @see ComparisonFunctionsVisitor
   */
  @Override
  public ResolvableExpression visitComparisonFunctions(VtlParser.ComparisonFunctionsContext ctx) {
    return comparisonFunctionsVisitor.visit(ctx.comparisonOperators());
  }

  /**
   * Visits set function expressions.
   *
   * @param ctx The scripting context for the function expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the function expression.
   * @see SetFunctionsVisitor
   */
  @Override
  public ResolvableExpression visitSetFunctions(VtlParser.SetFunctionsContext ctx) {
    return setFunctionsVisitor.visit(ctx.setOperators());
  }

  /**
   * Visits join function expressions.
   *
   * @param ctx The scripting context for the function expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the function expression.
   * @see JoinFunctionsVisitor
   */
  @Override
  public ResolvableExpression visitJoinFunctions(VtlParser.JoinFunctionsContext ctx) {
    return joinFunctionsVisitor.visitJoinFunctions(ctx);
  }

  /**
   * Visits numeric function expressions.
   *
   * @param ctx The scripting context for the function expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the function expression.
   * @see NumericFunctionsVisitor
   */
  @Override
  public ResolvableExpression visitGenericFunctions(VtlParser.GenericFunctionsContext ctx) {
    return genericFunctionsVisitor.visitGenericFunctions(ctx);
  }

  @Override
  public ResolvableExpression visitNumericFunctions(VtlParser.NumericFunctionsContext ctx) {
    return numericFunctionsVisitor.visit(ctx.numericOperators());
  }

  /**
   * Visits expressions involving distance functions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the distance function.
   * @see DistanceFunctionsVisitor
   */
  @Override
  public ResolvableExpression visitDistanceFunctions(VtlParser.DistanceFunctionsContext ctx) {
    return distanceFunctionsVisitor.visit(ctx.distanceOperators());
  }

  /**
   * Visits expressions involving time functions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the time function.
   * @see TimeFunctionsVisitor
   */
  @Override
  public ResolvableExpression visitTimeFunctions(VtlParser.TimeFunctionsContext ctx) {
    return timeFunctionsVisitor.visit(ctx.timeOperators());
  }

  /**
   * Visits expressions involving validation functions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the validation function.
   * @see TimeFunctionsVisitor
   */
  @Override
  public ResolvableExpression visitValidationFunctions(VtlParser.ValidationFunctionsContext ctx) {
    return validationFunctionsVisitor.visit(ctx.validationOperators());
  }

  /**
   * Visits clause expressions.
   *
   * @param ctx The scripting context for the expression.
   * @return A <code>ResolvableExpression</code> resolving to the result of the close expression.
   * @see ClauseVisitor
   */
  @Override
  public ResolvableExpression visitClauseExpr(VtlParser.ClauseExprContext ctx) {
    DatasetExpression datasetExpression = (DatasetExpression) visit(ctx.dataset);
    ClauseVisitor clauseVisitor = new ClauseVisitor(datasetExpression, processingEngine, engine);
    return clauseVisitor.visit(ctx.clause);
  }

  @Override
  public ResolvableExpression visitRatioToReportAn(VtlParser.RatioToReportAnContext ctx) {
    var datasetExpression = (DatasetExpression) visit(ctx.expr());
    var dsName = ctx.expr().getText();
    var structure = datasetExpression.getDataStructure();
    var ids = structure.getIdentifiers();

    Map<String, DatasetExpression> analytics = new LinkedHashMap<>();
    for (Structured.Component measure : structure.getMeasures()) {

      // Convert each measure to an equivalent [calc xx := analyticFunction].
      // where xx is [function name]_[original_measure_name]
      List<String> colNames =
          Stream.concat(ids.stream(), Stream.of(measure))
              .map(Structured.Component::getName)
              .collect(Collectors.toList());

      // Filter out measure we don't need and rename the measure to the name of
      // the dataset.
      var tempDs = processingEngine.executeProject(datasetExpression, colNames);
      tempDs = processingEngine.executeRename(tempDs, Map.of(measure.getName(), dsName));

      // Execute the calc.
      String targetColumnName = ctx.op.getText() + "_" + measure.getName();
      AnalyticsVisitor analyticsVisitor =
          new AnalyticsVisitor(processingEngine, tempDs, targetColumnName);
      var result = analyticsVisitor.visit(ctx);

      // Rename back to the original name.
      result = processingEngine.executeRename(result, Map.of(targetColumnName, measure.getName()));

      // Drop the ds1 column.
      result =
          processingEngine.executeProject(
              result,
              result.getColumnNames().stream()
                  .filter(n -> !n.equals(dsName))
                  .collect(Collectors.toList()));

      analytics.put(targetColumnName, result);
    }
    return processingEngine.executeInnerJoin(analytics);
  }

  @Override
  public ResolvableExpression visitLagOrLeadAn(VtlParser.LagOrLeadAnContext ctx) {
    var datasetExpression = (DatasetExpression) visit(ctx.expr());
    var dsName = ctx.expr().getText();
    var structure = datasetExpression.getDataStructure();
    var ids = structure.getIdentifiers();

    Map<String, DatasetExpression> analytics = new LinkedHashMap<>();
    for (Structured.Component measure : structure.getMeasures()) {

      // Convert each measure to an equivalent [calc xx := analyticFunction].
      // where xx is [function name]_[original_measure_name]
      List<String> colNames =
          Stream.concat(ids.stream(), Stream.of(measure))
              .map(Structured.Component::getName)
              .collect(Collectors.toList());

      // Filter out measure we don't need and rename the measure to the name of
      // the dataset.
      var tempDs = processingEngine.executeProject(datasetExpression, colNames);
      tempDs = processingEngine.executeRename(tempDs, Map.of(measure.getName(), dsName));

      // Execute the calc.
      String targetColumnName = ctx.op.getText() + "_" + measure.getName();
      AnalyticsVisitor analyticsVisitor =
          new AnalyticsVisitor(processingEngine, tempDs, targetColumnName);
      var result = analyticsVisitor.visit(ctx);

      // Rename back to the original name.
      result = processingEngine.executeRename(result, Map.of(targetColumnName, measure.getName()));

      // Drop the ds1 column.
      result =
          processingEngine.executeProject(
              result,
              result.getColumnNames().stream()
                  .filter(n -> !n.equals(dsName))
                  .collect(Collectors.toList()));

      analytics.put(targetColumnName, result);
    }
    return processingEngine.executeInnerJoin(analytics);
  }

  @Override
  public DatasetExpression visitAnSimpleFunction(VtlParser.AnSimpleFunctionContext ctx) {
    var datasetExpression = (DatasetExpression) visit(ctx.expr());
    var dsName = ctx.expr().getText();
    var structure = datasetExpression.getDataStructure();
    var ids = structure.getIdentifiers();

    Map<String, DatasetExpression> analytics = new LinkedHashMap<>();
    for (Structured.Component measure : structure.getMeasures()) {

      // Convert each measure to an equivalent [calc xx := analyticFunction].
      // where xx is [function name]_[original_measure_name]
      List<String> colNames =
          Stream.concat(ids.stream(), Stream.of(measure))
              .map(Structured.Component::getName)
              .collect(Collectors.toList());

      // Filter out measure we don't need and rename the measure to the name of
      // the dataset.
      var tempDs = processingEngine.executeProject(datasetExpression, colNames);
      tempDs = processingEngine.executeRename(tempDs, Map.of(measure.getName(), dsName));

      // Execute the calc.
      String targetColumnName = ctx.op.getText() + "_" + measure.getName();
      AnalyticsVisitor analyticsVisitor =
          new AnalyticsVisitor(processingEngine, tempDs, targetColumnName);
      var result = analyticsVisitor.visit(ctx);

      // Rename back to the original name.
      result = processingEngine.executeRename(result, Map.of(targetColumnName, measure.getName()));

      // Drop the ds1 column.
      result =
          processingEngine.executeProject(
              result,
              result.getColumnNames().stream()
                  .filter(n -> !n.equals(dsName))
                  .collect(Collectors.toList()));

      analytics.put(targetColumnName, result);
    }
    return processingEngine.executeInnerJoin(analytics);
  }

  @Override
  public ResolvableExpression visitFunctionsExpression(VtlParser.FunctionsExpressionContext ctx) {
    ResolvableExpression expr = super.visitFunctionsExpression(ctx);
    if (Objects.isNull(expr)) {
      VtlParser.FunctionsContext functionsContext = ctx.functions();
      String functionName = functionsContext.getStart().getText();
      throw new VtlRuntimeException(
          new UnimplementedException(
              "the function " + functionName + " is not yet implemented", fromContext(ctx)));
    }
    return expr;
  }
}
