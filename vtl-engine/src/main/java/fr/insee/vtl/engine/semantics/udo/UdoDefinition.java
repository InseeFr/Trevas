package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Runtime artefact for a user-defined operator. Stored in script bindings under the operator name.
 * The VTL body remains an ANTLR subtree and is evaluated by {@link
 * fr.insee.vtl.engine.expressions.UdoFunctionExpression} via {@link
 * fr.insee.vtl.engine.visitors.expression.ExpressionVisitor}.
 */
public final class UdoDefinition {

  private final String name;
  private final List<UdoParameter> parameters;
  private final Class<?> returnType;
  private final UdoDatasetSignature returnDatasetSignature;
  private final VtlParser.ExprContext body;
  private final VtlScriptEngine engine;
  private final Map<String, Object> closureBindings;

  public UdoDefinition(
      String name,
      List<UdoParameter> parameters,
      Class<?> returnType,
      VtlParser.ExprContext body,
      VtlScriptEngine engine) {
    this(name, parameters, returnType, null, body, engine, Map.of());
  }

  public UdoDefinition(
      String name,
      List<UdoParameter> parameters,
      Class<?> returnType,
      UdoDatasetSignature returnDatasetSignature,
      VtlParser.ExprContext body,
      VtlScriptEngine engine) {
    this(name, parameters, returnType, returnDatasetSignature, body, engine, Map.of());
  }

  public UdoDefinition(
      String name,
      List<UdoParameter> parameters,
      Class<?> returnType,
      VtlParser.ExprContext body,
      VtlScriptEngine engine,
      Map<String, Object> closureBindings) {
    this(name, parameters, returnType, null, body, engine, closureBindings);
  }

  public UdoDefinition(
      String name,
      List<UdoParameter> parameters,
      Class<?> returnType,
      UdoDatasetSignature returnDatasetSignature,
      VtlParser.ExprContext body,
      VtlScriptEngine engine,
      Map<String, Object> closureBindings) {
    this.name = Objects.requireNonNull(name);
    this.parameters = List.copyOf(parameters);
    this.returnType = returnType;
    this.returnDatasetSignature = returnDatasetSignature;
    this.body = Objects.requireNonNull(body);
    this.engine = Objects.requireNonNull(engine);
    this.closureBindings = Map.copyOf(closureBindings);
  }

  public String getName() {
    return name;
  }

  public List<UdoParameter> getParameters() {
    return parameters;
  }

  public Class<?> getReturnType() {
    return returnType;
  }

  public UdoDatasetSignature getReturnDatasetSignature() {
    return returnDatasetSignature;
  }

  public VtlParser.ExprContext getBody() {
    return body;
  }

  public VtlScriptEngine getEngine() {
    return engine;
  }

  public Map<String, Object> getClosureBindings() {
    return closureBindings;
  }
}
