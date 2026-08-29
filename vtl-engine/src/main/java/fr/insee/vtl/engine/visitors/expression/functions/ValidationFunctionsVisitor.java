package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;
import static fr.insee.vtl.engine.utils.TypeChecking.assertTypeExpression;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.semantics.validation.ValidationExecutor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.Objects;

/**
 * Visitor dispatch for validation expressions; orchestration lives in {@link ValidationExecutor}.
 */
public class ValidationFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

  private final ExpressionVisitor expressionVisitor;
  private final ProcessingEngine processingEngine;
  private final VtlScriptEngine engine;

  public ValidationFunctionsVisitor(
      ExpressionVisitor expressionVisitor,
      ProcessingEngine processingEngine,
      VtlScriptEngine engine) {
    this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    this.processingEngine = Objects.requireNonNull(processingEngine);
    this.engine = Objects.requireNonNull(engine);
  }

  @Override
  public ResolvableExpression visitValidateDPruleset(VtlParser.ValidateDPrulesetContext ctx) {
    String dprName = ctx.dpName.getText();
    DataPointRuleset dpr = resolveDataPointRuleset(dprName, fromContext(ctx));

    DatasetExpression ds =
        (DatasetExpression)
            assertTypeExpression(expressionVisitor.visit(ctx.op), Dataset.class, ctx.op);

    return ValidationExecutor.validateDataPointRuleset(
        processingEngine,
        dpr,
        ds,
        ctx.op.getText(),
        dprName,
        getValidationOutput(ctx.validationOutput()),
        fromContext(ctx));
  }

  @Override
  public ResolvableExpression visitValidationSimple(VtlParser.ValidationSimpleContext ctx) {
    var pos = fromContext(ctx);
    DatasetExpression dsExpression =
        (DatasetExpression)
            assertTypeExpression(expressionVisitor.visit(ctx.expr()), Dataset.class, ctx.expr());
    ResolvableExpression erCodeExpression =
        ctx.erCode() != null ? expressionVisitor.visit(ctx.erCode()) : null;
    ResolvableExpression erLevelExpression =
        ctx.erLevel() != null ? expressionVisitor.visit(ctx.erLevel()) : null;
    DatasetExpression imbalanceExpression = null;
    if (ctx.imbalanceExpr() != null) {
      imbalanceExpression =
          (DatasetExpression)
              assertTypeExpression(
                  expressionVisitor.visit(ctx.imbalanceExpr()), Dataset.class, ctx.imbalanceExpr());
    }

    return ValidationExecutor.validateSimple(
        processingEngine,
        dsExpression,
        erCodeExpression,
        erLevelExpression,
        imbalanceExpression,
        ctx.output != null ? ctx.output.getText() : null,
        pos);
  }

  @Override
  public ResolvableExpression visitValidateHRruleset(VtlParser.ValidateHRrulesetContext ctx) {
    var pos = fromContext(ctx);
    DatasetExpression dsExpression =
        (DatasetExpression)
            assertTypeExpression(expressionVisitor.visit(ctx.expr()), Dataset.class, ctx.expr());
    String hrName = ctx.hrName.getText();
    HierarchicalRuleset hr = resolveHierarchicalRuleset(hrName, pos);

    return ValidationExecutor.validateHierarchical(
        processingEngine,
        dsExpression,
        hr,
        ctx.expr().getText(),
        ctx.componentID().getText(),
        getValidationMode(ctx.validationMode()),
        getInputMode(ctx.inputMode()),
        getValidationOutput(ctx.validationOutput()),
        pos);
  }

  private DataPointRuleset resolveDataPointRuleset(String name, Positioned pos) {
    Object binding = expressionVisitor.lookupBinding(name);
    if (binding instanceof DataPointRuleset dpr) {
      return dpr;
    }
    Object fromEngine = engine.getContext().getAttribute(name);
    if (fromEngine instanceof DataPointRuleset dpr) {
      return dpr;
    }
    throw new VtlRuntimeException(new UndefinedVariableException(name, pos));
  }

  private HierarchicalRuleset resolveHierarchicalRuleset(String name, Positioned pos) {
    Object binding = expressionVisitor.lookupBinding(name);
    if (binding instanceof HierarchicalRuleset hr) {
      return hr;
    }
    Object fromEngine = engine.getContext().getAttribute(name);
    if (fromEngine instanceof HierarchicalRuleset hr) {
      return hr;
    }
    throw new VtlRuntimeException(new UndefinedVariableException(name, pos));
  }

  private String getValidationOutput(VtlParser.ValidationOutputContext voc) {
    return voc != null ? voc.getText() : null;
  }

  private String getValidationMode(VtlParser.ValidationModeContext vmc) {
    return vmc != null ? vmc.getText() : null;
  }

  private String getInputMode(VtlParser.InputModeContext imc) {
    return imc != null ? imc.getText() : null;
  }
}
