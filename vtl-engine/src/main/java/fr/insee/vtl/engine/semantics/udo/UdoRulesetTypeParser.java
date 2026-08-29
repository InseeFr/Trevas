package fr.insee.vtl.engine.semantics.udo;

import fr.insee.vtl.engine.exceptions.UnimplementedException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlParser;

/** Parses {@code ruleset}, {@code datapoint}, and {@code hierarchical} UDO parameter types. */
final class UdoRulesetTypeParser {

  private UdoRulesetTypeParser() {}

  static UdoRulesetKind parse(VtlParser.RulesetTypeContext ctx, Positioned pos)
      throws VtlScriptException {
    if (ctx.RULESET() != null) {
      return UdoRulesetKind.ANY;
    }
    if (ctx.dpRuleset() != null) {
      var dp = ctx.dpRuleset();
      if (dp instanceof VtlParser.DataPointContext) {
        return UdoRulesetKind.DATAPOINT;
      }
      throw new VtlRuntimeException(
          new UnimplementedException(
              "UDO datapoint ruleset constraints (on variable / valuedomain) not supported yet",
              pos));
    }
    if (ctx.hrRuleset() != null) {
      var hr = ctx.hrRuleset();
      if (hr instanceof VtlParser.HrRulesetTypeContext) {
        return UdoRulesetKind.HIERARCHICAL;
      }
      throw new VtlRuntimeException(
          new UnimplementedException(
              "UDO hierarchical ruleset constraints (on variable / valuedomain) not supported yet",
              pos));
    }
    throw new VtlRuntimeException(
        new UnimplementedException("UDO ruleset type not supported: " + ctx.getText(), pos));
  }
}
