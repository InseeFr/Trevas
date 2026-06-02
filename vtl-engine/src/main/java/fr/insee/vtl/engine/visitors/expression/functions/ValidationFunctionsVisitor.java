package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <code>ValidationFunctionsVisitor</code> is the base visitor for expressions involving validation
 * functions.
 */
public class ValidationFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor expressionVisitor;
  private final ProcessingEngine processingEngine;
  private final VtlScriptEngine engine;

  /**
   * Constructor taking an expression visitor and a processing engine.
   *
   * @param expressionVisitor A visitor for the expression corresponding to the validation function.
   * @param processingEngine The processing engine.
   */
  public ValidationFunctionsVisitor(
      ExpressionVisitor expressionVisitor,
      ProcessingEngine processingEngine,
      VtlScriptEngine engine) {
    this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    this.processingEngine = Objects.requireNonNull(processingEngine);
    this.engine = Objects.requireNonNull(engine);
  }

  /**
   * Visits a DataPointRuleset expression to validate.
   *
   * @param ctx The scripting context for the expression...
   * @return A <code>ResolvableExpression</code> resolving to...
   */
  @Override
  public ResolvableExpression visitValidateDPruleset(VtlParser.ValidateDPrulesetContext ctx) {
    // get DataPointRuleset
    String dprName = ctx.dpName.getText();
    Object dprObject = engine.getContext().getAttribute((dprName));
    String output = getValidationOutput(ctx.validationOutput());
    if (!(dprObject instanceof DataPointRuleset))
      throw new VtlRuntimeException(new UndefinedVariableException(dprName, fromContext(ctx)));
    DataPointRuleset dpr = (DataPointRuleset) dprObject;

    DatasetExpression ds =
        (DatasetExpression)
            assertTypeExpression(expressionVisitor.visit(ctx.op), Dataset.class, ctx.op);

    List<String> valuedomaines = new ArrayList<>();
    Map<String, ResolvableExpression> colsToAdd = new HashMap<>();

    // Map valuedomains to variables and update variable alias
    DatasetExpression finalDs = ds;
    dpr.getValuedomains()
        .forEach(
            vd -> {
              List<String> vars =
                  finalDs.getDataStructure().getByValuedomain(vd).stream()
                      .map(Structured.Component::getName)
                      .toList();
              if (vars.isEmpty()) {
                throw new VtlRuntimeException(
                    new InvalidArgumentException(
                        "Valuedomain " + vd + " not used in " + ctx.op.getText() + " components",
                        fromContext(ctx)));
              }
              if (vars.size() > 1) {
                throw new VtlRuntimeException(
                    new InvalidArgumentException(
                        "Valuedomain "
                            + vd
                            + " is used by "
                            + vars.size()
                            + " components in "
                            + ctx.op.getText(),
                        fromContext(ctx)));
              }
              List<String> newList =
                  Stream.concat(dpr.getVariables().stream(), vars.stream())
                      .collect(Collectors.toList());
              dpr.setVariables(newList);

              valuedomaines.add(vd);
              Class targetClass = finalDs.getDataStructure().get(vars.get(0)).getType();
              colsToAdd.put(
                  vd,
                  ResolvableExpression.withType(targetClass)
                      .withPosition(fromContext(ctx))
                      .using(
                          c -> {
                            Map<String, Object> mapContext = (Map<String, Object>) c;
                            return mapContext.get(vars.get(0));
                          }));
            });

    // Temp create vd column
    ds = processingEngine.executeCalc(ds, colsToAdd, Map.of(), Map.of());

    // check if dpr variables are in ds structure
    Structured.DataStructure dataStructure = ds.getDataStructure();
    dpr.getVariables()
        .forEach(
            v -> {
              if (!dataStructure.containsKey(v)) {
                throw new VtlRuntimeException(
                    new InvalidArgumentException(
                        "Variable " + v + " not contained in " + ctx.op.getText(),
                        fromContext(ctx)));
              }
            });

    // check if dpr alias are not in ds
    dpr.getAlias()
        .values()
        .forEach(
            v -> {
              if (dataStructure.containsKey(v)) {
                throw new VtlRuntimeException(
                    new InvalidArgumentException(
                        "Alias "
                            + v
                            + " from "
                            + dprName
                            + " ruleset already defined in "
                            + ctx.op.getText(),
                        fromContext(ctx)));
              }
            });

    var pos = fromContext(ctx);

    return processingEngine.executeValidateDPruleset(dpr, ds, output, pos, valuedomaines);
  }

  /**
   * Visits a datasets to validate.
   *
   * @param ctx The scripting context for the expression...
   * @return A <code>ResolvableExpression</code> resolving to...
   */
  @Override
  public ResolvableExpression visitValidationSimple(VtlParser.ValidationSimpleContext ctx) {
    var pos = fromContext(ctx);
    DatasetExpression dsExpression =
        (DatasetExpression)
            assertTypeExpression(expressionVisitor.visit(ctx.expr()), Dataset.class, ctx.expr());
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
    ResolvableExpression erCodeExpression =
        null != ctx.erCode() ? expressionVisitor.visit(ctx.erCode()) : null;
    ResolvableExpression erLevelExpression =
        null != ctx.erLevel() ? expressionVisitor.visit(ctx.erLevel()) : null;
    DatasetExpression imbalanceExpression =
        (DatasetExpression)
            assertTypeExpression(
                expressionVisitor.visit(ctx.imbalanceExpr()), Dataset.class, ctx.imbalanceExpr());
    if (null != imbalanceExpression) {
      List<Structured.Component> imbalanceMeasures =
          imbalanceExpression.getDataStructure().values().stream()
              .filter(Structured.Component::isMeasure)
              .toList();
      if (imbalanceMeasures.size() != 1) {
        throw new VtlRuntimeException(
            new InvalidArgumentException("Check imbalance dataset contains several measures", pos));
      }
      List<Class<?>> supportedClasses = new ArrayList<>(Arrays.asList(Double.class, Long.class));
      if (!supportedClasses.contains(imbalanceMeasures.get(0).getType())) {
        throw new VtlRuntimeException(
            new InvalidArgumentException("Check imbalance dataset measure has to be numeric", pos));
      }
    }
    String output = ctx.output != null ? ctx.output.getText() : null;
    return processingEngine.executeValidationSimple(
        dsExpression, erCodeExpression, erLevelExpression, imbalanceExpression, output, pos);
  }

  // TODO: handle other IDs than componentID? build unique ID tuples to calculate
  @Override
  public ResolvableExpression visitValidateHRruleset(VtlParser.ValidateHRrulesetContext ctx) {
    var pos = fromContext(ctx);
    DatasetExpression dsExpression =
        (DatasetExpression)
            assertTypeExpression(expressionVisitor.visit(ctx.expr()), Dataset.class, ctx.expr());
    String datasetName = ctx.expr().getText();
    String hrName = ctx.hrName.getText();
    Object hrObject = engine.getContext().getAttribute((hrName));
    if (!(hrObject instanceof HierarchicalRuleset))
      throw new VtlRuntimeException(new UndefinedVariableException(hrName, pos));
    HierarchicalRuleset hr = (HierarchicalRuleset) hrObject;

    // Check that dsE is a monomeasure<number> dataset
    Structured.DataStructure dataStructure = dsExpression.getDataStructure();
    List<Structured.Component> measures = dataStructure.getMeasures();
    if (measures.size() != 1) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "Dataset " + datasetName + " is not monomeasure", fromContext(ctx)));
    }
    List<Class<?>> supportedClasses = new ArrayList<>(Arrays.asList(Double.class, Long.class));
    if (!supportedClasses.contains(measures.get(0).getType())) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "Dataset "
                  + datasetName
                  + " measure "
                  + measures.get(0).getName()
                  + " has to have number type",
              fromContext(ctx)));
    }

    // check if hr componentID is in ds structure
    String componentID = ctx.componentID().getText();
    if (!dataStructure.containsKey(componentID)) {
      throw new VtlRuntimeException(
          new InvalidArgumentException(
              "ComponentID " + componentID + " not contained in dataset " + datasetName,
              fromContext(ctx)));
    }
    String validationMode = getValidationMode(ctx.validationMode());
    String inputMode = getInputMode(ctx.inputMode());
    String validationOutput = getValidationOutput(ctx.validationOutput());
    return processingEngine.executeHierarchicalValidation(
        dsExpression, hr, componentID, validationMode, inputMode, validationOutput, pos);
  }

  private String getValidationOutput(VtlParser.ValidationOutputContext voc) {
    if (null == voc) return null;
    return voc.getText();
  }

  private String getValidationMode(VtlParser.ValidationModeContext vmc) {
    if (null == vmc) return null;
    return vmc.getText();
  }

  private String getInputMode(VtlParser.InputModeContext imc) {
    if (null == imc) return null;
    return imc.getText();
  }
}
