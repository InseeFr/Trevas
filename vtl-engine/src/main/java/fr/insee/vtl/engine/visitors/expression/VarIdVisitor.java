package fr.insee.vtl.engine.visitors.expression;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.expressions.ComponentExpression;
import fr.insee.vtl.model.ConstantExpression;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/** <code>VarIdVisitor</code> is the base visitor for variable identifiers. */
public class VarIdVisitor extends VtlBaseVisitor<ResolvableExpression> implements Serializable {

  private final Map<String, Object> context;

  /**
   * Constructor taking a scripting context.
   *
   * @param context The context for the visitor.
   */
  public VarIdVisitor(Map<String, Object> context) {
    this.context = Objects.requireNonNull(context);
  }

  @Override
  public ResolvableExpression visitVarID(VtlParser.VarIDContext ctx) {
    final String variableName = ctx.getText();
    var pos = fromContext(ctx);

    if (!context.containsKey(variableName)) {
      throw new VtlRuntimeException(new UndefinedVariableException(variableName, pos));
    }

    Object value = context.get(variableName);
    if (value instanceof Dataset dataset) {
      return DatasetExpression.of(dataset, pos);
    }

    if (value instanceof Structured.Component component) {
      return new ComponentExpression(component, pos);
    }

    if (value instanceof Integer integer) {
      value = Long.valueOf(integer);
    }

    if (value instanceof Float float1) {
      value = Double.valueOf(float1);
    }

    if (value == null) {
      return ResolvableExpression.withType(Object.class)
          .withPosition(pos)
          .using(c -> c.get(variableName));
    }

    return new ConstantExpression(value, pos);
  }
}
