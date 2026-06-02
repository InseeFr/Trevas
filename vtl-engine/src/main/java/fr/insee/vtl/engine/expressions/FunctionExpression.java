package fr.insee.vtl.engine.expressions;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.ListExpression;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.VtlMethod;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** An expression that calls a method. */
public class FunctionExpression extends ResolvableExpression {

  private final VtlMethod method;

  private final Class<?> returnType;
  private final List<ResolvableExpression> parameters;

  public FunctionExpression(
      VtlMethod method, List<ResolvableExpression> parameters, Positioned position)
      throws VtlScriptException {
    super(position);
    this.method = Objects.requireNonNull(method);
    this.parameters = Objects.requireNonNull(parameters);
    this.returnType = this.method.getMethod(position).getReturnType();

    var expectedTypes = Arrays.asList(this.method.getMethod(position).getParameterTypes());
    if (expectedTypes.size() < parameters.size()) {
      throw new VtlScriptException("unexpected parameter", parameters.get(expectedTypes.size()));
    } else if (expectedTypes.size() > parameters.size()) {
      throw new VtlScriptException("missing parameter", position);
    }

    var exprIt = parameters.iterator();
    var typeIt = expectedTypes.iterator();
    while (exprIt.hasNext() && typeIt.hasNext()) {
      var expression = exprIt.next();
      var type = typeIt.next();
      if (type.equals(Object.class)) {
        continue;
      }
      if (type.isAssignableFrom(List.class)) {
        ((ListExpression) expression).containedType();
      }
      expression.checkInstanceOf(type);
    }
  }

  @Override
  public Object resolve(Map<String, Object> context) {
    Object[] evaluatedParameters = parameters.stream().map(p -> p.resolve(context)).toArray();
    try {
      return method.getMethod(this).invoke(null, evaluatedParameters);
    } catch (InvocationTargetException ite) {
      var cause = ite.getCause();
      if (cause instanceof Exception) {
        throw new VtlRuntimeException(new VtlScriptException((Exception) ite.getCause(), this));
      } else {
        throw new VtlRuntimeException(new VtlScriptException(new Exception(ite.getCause()), this));
      }
    } catch (Exception e) {
      throw new VtlRuntimeException(new VtlScriptException(e, this));
    }
  }

  @Override
  public Class<?> getType() {
    return this.returnType;
  }
}
