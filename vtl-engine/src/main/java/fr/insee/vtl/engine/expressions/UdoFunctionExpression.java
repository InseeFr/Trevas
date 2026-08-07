package fr.insee.vtl.engine.expressions;

import fr.insee.vtl.engine.semantics.udo.UdoDefinition;
import fr.insee.vtl.engine.semantics.udo.UdoTrampoline;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.VtlMethod;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.List;
import java.util.Map;

/**
 * {@link FunctionExpression} specialised for UDOs: sets the trampoline {@link
 * UdoTrampoline.CallSite} around {@code Method.invoke} so the reflective call can re-enter the VTL
 * body with the correct definition and outer bindings.
 */
public final class UdoFunctionExpression extends FunctionExpression {

  private final UdoDefinition udo;
  private final Class<?> declaredType;

  public UdoFunctionExpression(
      UdoDefinition udo, List<ResolvableExpression> parameters, Positioned position)
      throws VtlScriptException {
    super(new VtlMethod(UdoTrampoline.methodForArity(parameters.size())), parameters, position);
    this.udo = udo;
    this.declaredType = udo.getReturnType() != null ? udo.getReturnType() : Object.class;
  }

  @Override
  public Object resolve(Map<String, Object> context) {
    UdoTrampoline.enter(udo, context);
    try {
      return super.resolve(context);
    } finally {
      UdoTrampoline.exit();
    }
  }

  @Override
  public Class<?> getType() {
    return declaredType;
  }
}
