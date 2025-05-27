package fr.insee.vtl.prov;

import java.util.Optional;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

public class Variable {

  /** Returns the text of a context with empty space (aka. all channels). */
  private static String getText(ParserRuleContext ctx) {
    Token start = ctx.getStart();
    Token stop = ctx.getStop();
    return start.getInputStream().getText(new Interval(start.getStartIndex(), stop.getStopIndex()));
  }

  // Identifier of the variable.
  private final ParserRuleContext nameCtx;

  // Expression of the variable, if any.
  private final ParserRuleContext exprCtx;

  // Version of the variable.
  private Integer version = 0;

  // Previous version.
  private Variable previous;

  Variable(ParserRuleContext name, ParserRuleContext expression) {
    this.nameCtx = name;
    this.exprCtx = expression;
  }

  public Variable(ParserRuleContext name) {
    this.nameCtx = name;
    this.exprCtx = null;
  }

  public String getName() {
    return getText(this.nameCtx);
  }

  public String getVersion() {
    return getName() + "_" + this.version;
  }

  public Optional<String> getExpression() {
    return Optional.ofNullable(exprCtx).map(Variable::getText);
  }

  public Optional<Variable> getPrevious() {
    return Optional.ofNullable(previous);
  }

  public Variable newVersion() {
    Variable copy = new Variable(this.nameCtx, this.exprCtx);
    copy.version = this.version + 1;
    copy.previous = this;
    return copy;
  }

  @Override
  public String toString() {
    return getVersion();
  }

  public final boolean isSame(Variable other) {
    return nameCtx.equals(other.nameCtx);
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Variable)) return false;

    Variable variable = (Variable) o;
    return getVersion().equals(variable.getVersion());
  }

  @Override
  public int hashCode() {
    return nameCtx.hashCode();
  }
}
