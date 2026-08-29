package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ConstantVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.script.ScriptContext;

/** Builds {@link UdoDefinition} from a define-operator parse tree. */
public final class UdoDefineExecutor {

  private static final ConstantVisitor CONSTANTS = new ConstantVisitor();

  private UdoDefineExecutor() {}

  public static UdoDefinition define(VtlParser.DefOperatorContext ctx, VtlScriptEngine engine)
      throws VtlScriptException {
    String name = ctx.operatorID().getText();
    Positioned pos = VtlScriptEngine.fromContext(ctx);

    List<UdoParameter> parameters = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    if (ctx.parameterItem() != null) {
      for (VtlParser.ParameterItemContext item : ctx.parameterItem()) {
        String paramName = item.varID().getText();
        if (!seen.add(paramName)) {
          throw new VtlScriptException("duplicate UDO parameter '" + paramName + "'", pos);
        }
        parameters.add(parseParameter(item, pos));
      }
    }

    Class<?> returnType = null;
    UdoDatasetSignature returnDatasetSignature = null;
    if (ctx.outputParameterType() != null) {
      if (ctx.outputParameterType().datasetType() != null) {
        returnDatasetSignature =
            UdoDatasetTypeParser.parse(ctx.outputParameterType().datasetType(), pos);
        returnType = Dataset.class;
      } else {
        returnType = parseOutputType(ctx.outputParameterType(), pos);
      }
    }

    return new UdoDefinition(
        name,
        parameters,
        returnType,
        returnDatasetSignature,
        ctx.expr(),
        engine,
        UdoClosureBindings.capture(ctx, engine.getBindings(ScriptContext.ENGINE_SCOPE)));
  }

  private static UdoParameter parseParameter(VtlParser.ParameterItemContext item, Positioned pos)
      throws VtlScriptException {
    String paramName = item.varID().getText();
    UdoDatasetSignature datasetSignature = null;
    Dataset.Role componentRole = null;
    Class<?> componentScalarType = null;
    UdoRulesetKind rulesetKind = null;
    Class<?> type;
    if (item.inputParameterType().datasetType() != null) {
      datasetSignature = UdoDatasetTypeParser.parse(item.inputParameterType().datasetType(), pos);
      type = Dataset.class;
    } else if (item.inputParameterType().componentType() != null) {
      var signature = UdoComponentTypeParser.parse(item.inputParameterType().componentType(), pos);
      componentRole = signature.role();
      componentScalarType = signature.scalarType();
      type = Structured.Component.class;
    } else if (item.inputParameterType().rulesetType() != null) {
      rulesetKind = UdoRulesetTypeParser.parse(item.inputParameterType().rulesetType(), pos);
      type = UdoRulesetBinding.TYPE;
    } else {
      type = parseInputType(item.inputParameterType(), pos);
    }
    if (item.constant() != null) {
      var constant = CONSTANTS.visit(item.constant());
      Object value = constant.resolve(java.util.Map.of());
      if (value != null && !UdoTypes.isAssignable(type, value.getClass())) {
        throw new VtlScriptException(
            "default value type does not match parameter type " + UdoTypes.vtlTypeName(type), pos);
      }
      return UdoParameter.withDefaultParsed(
          paramName,
          type,
          datasetSignature,
          componentRole,
          componentScalarType,
          rulesetKind,
          value);
    }
    return UdoParameter.mandatoryParsed(
        paramName, type, datasetSignature, componentRole, componentScalarType, rulesetKind);
  }

  static Class<?> parseInputType(VtlParser.InputParameterTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    if (ctx.scalarType() != null) {
      return UdoTypes.parseScalarType(ctx.scalarType(), pos);
    }
    if (ctx.datasetType() != null) {
      return Dataset.class;
    }
    if (ctx.scalarSetType() != null) {
      throw new VtlRuntimeException(
          new UnimplementedException("UDO scalar set parameters not supported yet", pos));
    }
    throw new VtlRuntimeException(
        new UnimplementedException("UDO parameter type not supported yet: " + ctx.getText(), pos));
  }

  static Class<?> parseOutputType(VtlParser.OutputParameterTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    if (ctx.scalarType() != null) {
      return UdoTypes.parseScalarType(ctx.scalarType(), pos);
    }
    if (ctx.datasetType() != null) {
      return Dataset.class;
    }
    throw new VtlRuntimeException(
        new UnimplementedException("UDO return type not supported yet: " + ctx.getText(), pos));
  }
}
