package fr.insee.vtl.engine.semantics.udo;

/**
 * Marker type for UDO formal {@code ruleset} parameters. Runtime values are {@link
 * fr.insee.vtl.model.DataPointRuleset} or {@link fr.insee.vtl.model.HierarchicalRuleset}.
 */
public final class UdoRulesetBinding {

  private UdoRulesetBinding() {}

  public static final Class<UdoRulesetBinding> TYPE = UdoRulesetBinding.class;
}
