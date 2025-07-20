package fr.insee.vtl.engine.visitors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import org.antlr.v4.runtime.tree.TerminalNode;

/** <code>AssignmentVisitor</code> is the visitor for VTL assignment expressions. */
public class AssignmentVisitor extends VtlBaseVisitor<Object> {

  private final VtlScriptEngine engine;
  private final ProcessingEngine processingEngine;
  private final ExpressionVisitor expressionVisitor;

  /**
   * Constructor taking a scripting engine and a processing engine.
   *
   * @param engine The scripting engine.
   * @param processingEngine The processing engine.
   */
  public AssignmentVisitor(VtlScriptEngine engine, ProcessingEngine processingEngine) {
    this.engine = Objects.requireNonNull(engine);
    this.processingEngine = Objects.requireNonNull(processingEngine);
    expressionVisitor =
        new ExpressionVisitor(
            engine.getBindings(ScriptContext.ENGINE_SCOPE), processingEngine, engine);
  }

  private Object visitAssignment(VtlParser.ExprContext expr) {
    ResolvableExpression resolvableExpression = expressionVisitor.visit(expr);
    return resolvableExpression.resolve(engine.getBindings(ScriptContext.ENGINE_SCOPE));
  }

  @Override
  public Object visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
    var result = visitAssignment(ctx.expr());
    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
    String variableIdentifier = ctx.varID().getText();
    bindings.put(variableIdentifier, result);
    return result;
  }

  @Override
  public Object visitPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
    var result = visitAssignment(ctx.expr());
    if (result instanceof Dataset resultDataset) {
      result = new PersistentDataset(resultDataset);
      Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
      String variableIdentifier = ctx.varID().getText();
      bindings.put(variableIdentifier, result);
      return result;
    }
    throw new VtlRuntimeException(
        new InvalidTypeException(Dataset.class, result.getClass(), fromContext(ctx)));
  }

  @Override
  public Object visitDefDatapointRuleset(VtlParser.DefDatapointRulesetContext ctx) {
    var pos = fromContext(ctx);
    String rulesetName = ctx.rulesetID().getText();
    List<VtlParser.SignatureContext> signature = ctx.rulesetSignature().signature();
    List<String> variables =
        ctx.rulesetSignature().VARIABLE() != null
            ? signature.stream().map(s -> s.varID().getText()).collect(Collectors.toList())
            : List.of();
    List<String> valuedomains =
        ctx.rulesetSignature().VALUE_DOMAIN() != null
            ? signature.stream().map(s -> s.varID().getText()).collect(Collectors.toList())
            : List.of();
    Map<String, String> alias =
        signature.stream()
            .filter(s -> null != s.alias())
            .collect(Collectors.toMap(k -> k.varID().getText(), v -> v.alias().getText()));

    Set<Class> erCodeTypes =
        ctx.ruleClauseDatapoint().ruleItemDatapoint().stream()
            .map(
                c -> {
                  VtlParser.ErCodeContext erCodeContext = c.erCode();
                  if (null == erCodeContext) return Object.class;
                  return expressionVisitor.visit(c.erCode()).getType();
                })
            .collect(Collectors.toSet());
    List<Class> filteredErCodeTypes =
        erCodeTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
    if (filteredErCodeTypes.size() > 1) {
      throw new VtlRuntimeException(
          new InvalidArgumentException("Error codes of rules have different types", pos));
    }
    Class erCodeType =
        filteredErCodeTypes.isEmpty() ? String.class : filteredErCodeTypes.iterator().next();

    Set<Class> erLevelTypes =
        ctx.ruleClauseDatapoint().ruleItemDatapoint().stream()
            .map(
                c -> {
                  VtlParser.ErLevelContext erLevelContext = c.erLevel();
                  if (null == erLevelContext) return Object.class;
                  return expressionVisitor.visit(c.erLevel()).getType();
                })
            .collect(Collectors.toSet());
    List<Class> filteredErLevelTypes =
        erLevelTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
    if (filteredErLevelTypes.size() > 1) {
      throw new VtlRuntimeException(
          new InvalidArgumentException("Error levels of rules have different types", pos));
    }
    Class erLevelType =
        filteredErLevelTypes.isEmpty() ? Long.class : filteredErLevelTypes.iterator().next();

    AtomicInteger index = new AtomicInteger();
    List<DataPointRule> rules =
        ctx.ruleClauseDatapoint().ruleItemDatapoint().stream()
            .map(
                c -> {
                  TerminalNode identifier = c.IDENTIFIER();
                  int i = index.getAndIncrement() + 1;
                  String name = null != identifier ? identifier.getText() : rulesetName + "_" + i;

                  VtlParser.ExprContext antecedentContiditonContext = c.antecedentContiditon;
                  VtlParser.ExprContext consequentConditionContext = c.consequentCondition;

                  ResolvableExpression errorCodeExpression =
                      null != c.erCode() ? expressionVisitor.visit(c.erCode()) : null;
                  ResolvableExpression errorLevelExpression =
                      null != c.erLevel() ? expressionVisitor.visit(c.erLevel()) : null;
                  return new DataPointRule(
                      name,
                      dataStructure -> {
                        if (antecedentContiditonContext != null) {
                          Map<String, Object> componentMap =
                              dataStructure.values().stream()
                                  .collect(
                                      Collectors.toMap(
                                          Dataset.Component::getName, component -> component));
                          return new ExpressionVisitor(componentMap, processingEngine, engine)
                              .visit(antecedentContiditonContext);
                        } else {
                          return ResolvableExpression.withType(Boolean.class)
                              .withPosition(pos)
                              .using(cc -> Boolean.TRUE);
                        }
                      },
                      dataStructure -> {
                        Map<String, Object> componentMap =
                            dataStructure.values().stream()
                                .collect(
                                    Collectors.toMap(
                                        Dataset.Component::getName, component -> component));
                        return new ExpressionVisitor(componentMap, processingEngine, engine)
                            .visit(consequentConditionContext);
                      },
                      errorCodeExpression,
                      errorLevelExpression);
                })
            .collect(Collectors.toList());
    DataPointRuleset dataPointRuleset =
        new DataPointRuleset(
            rulesetName, rules, variables, valuedomains, alias, erCodeType, erLevelType);
    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
    bindings.put(rulesetName, dataPointRuleset);
    return dataPointRuleset;
  }

  // TODO: handle when clause (expr ctx)
  @Override
  public Object visitDefHierarchical(VtlParser.DefHierarchicalContext ctx) {
    var pos = fromContext(ctx);
    String rulesetName = ctx.rulesetID().getText();

    // Mix variables and valuedomain. Information useless for now, find use case to do so
    String variable = ctx.hierRuleSignature().IDENTIFIER().getText();

    Set<Class<?>> erCodeTypes =
        ctx.ruleClauseHierarchical().ruleItemHierarchical().stream()
            .map(
                c -> {
                  VtlParser.ErCodeContext erCodeContext = c.erCode();
                  if (null == erCodeContext) return Object.class;
                  return expressionVisitor.visit(c.erCode()).getType();
                })
            .collect(Collectors.toSet());
    List<Class<?>> filteredErCodeTypes =
        erCodeTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
    if (filteredErCodeTypes.size() > 1) {
      throw new VtlRuntimeException(
          new InvalidArgumentException("Error codes of rules have different types", pos));
    }
    Class<?> erCodeType =
        filteredErCodeTypes.isEmpty() ? String.class : filteredErCodeTypes.iterator().next();

    Set<Class<?>> erLevelTypes =
        ctx.ruleClauseHierarchical().ruleItemHierarchical().stream()
            .map(
                c -> {
                  VtlParser.ErLevelContext erLevelContext = c.erLevel();
                  if (null == erLevelContext) return Object.class;
                  return expressionVisitor.visit(c.erLevel()).getType();
                })
            .collect(Collectors.toSet());
    List<Class<?>> filteredErLevelTypes =
        erLevelTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
    if (filteredErLevelTypes.size() > 1) {
      throw new VtlRuntimeException(
          new InvalidArgumentException("Error levels of rules have different types", pos));
    }
    Class<?> erLevelType =
        filteredErLevelTypes.isEmpty() ? Long.class : filteredErLevelTypes.iterator().next();

    AtomicInteger index = new AtomicInteger();
    List<HierarchicalRule> rules =
        ctx.ruleClauseHierarchical().ruleItemHierarchical().stream()
            .map(
                r -> {
                  TerminalNode identifier = r.IDENTIFIER();
                  int i = index.getAndIncrement() + 1;
                  String ruleName =
                      null != identifier ? identifier.getText() : rulesetName + "_" + i;

                  List<String> codeItems = new ArrayList<>();
                  VtlParser.CodeItemRelationContext codeItemRelationContext = r.codeItemRelation();
                  String valueDomainValue =
                      codeItemRelationContext.valueDomainValue().IDENTIFIER().getText();
                  codeItems.add(valueDomainValue);

                  VtlParser.ComparisonOperandContext comparisonOperandContext =
                      codeItemRelationContext.comparisonOperand();

                  StringBuilder codeItemExpressionBuilder = new StringBuilder();
                  codeItemRelationContext
                      .codeItemRelationClause()
                      .forEach(
                          circ -> {
                            TerminalNode minus = circ.MINUS();
                            String rightCodeItem = circ.rightCodeItem.getText();
                            codeItems.add(rightCodeItem);
                            if (minus != null)
                              codeItemExpressionBuilder.append(" -").append(rightCodeItem);
                            // plus value or plus null & minus null mean plus
                            codeItemExpressionBuilder.append(" +").append(rightCodeItem);
                          });

                  String rightExpressionToEval = codeItemExpressionBuilder.toString();
                  String expressionToEval =
                      "bool_var := "
                          + valueDomainValue
                          + " "
                          + comparisonOperandContext.getText()
                          + " "
                          + rightExpressionToEval
                          + ";";

                  ResolvableExpression leftExpression =
                      ResolvableExpression.withType(Double.class)
                          .withPosition(pos)
                          .using(
                              context -> {
                                Bindings bindings = new SimpleBindings(context);
                                bindings.forEach(
                                    (k, v) ->
                                        engine
                                            .getContext()
                                            .setAttribute(k, v, ScriptContext.ENGINE_SCOPE));
                                try {
                                  engine.eval("left := " + valueDomainValue + ";");
                                  Object left = engine.getContext().getAttribute("left");
                                  engine
                                      .getContext()
                                      .removeAttribute("left", ScriptContext.ENGINE_SCOPE);
                                  bindings
                                      .keySet()
                                      .forEach(
                                          k ->
                                              engine
                                                  .getContext()
                                                  .removeAttribute(k, ScriptContext.ENGINE_SCOPE));
                                  if (left.getClass().isAssignableFrom(Double.class)) {
                                    return (Double) left;
                                  }
                                  return ((Long) left).doubleValue();
                                } catch (ScriptException e) {
                                  throw new VtlRuntimeException(
                                      new VtlScriptException(
                                          "right hierarchical rule has to return long or double",
                                          pos));
                                }
                              });

                  ResolvableExpression rightExpression =
                      ResolvableExpression.withType(Double.class)
                          .withPosition(pos)
                          .using(
                              context -> {
                                Bindings bindings = new SimpleBindings(context);
                                bindings.forEach(
                                    (k, v) ->
                                        engine
                                            .getContext()
                                            .setAttribute(k, v, ScriptContext.ENGINE_SCOPE));
                                try {
                                  engine.eval("right := " + rightExpressionToEval + ";");
                                  Object right = engine.getContext().getAttribute("right");
                                  engine
                                      .getContext()
                                      .removeAttribute("right", ScriptContext.ENGINE_SCOPE);
                                  bindings
                                      .keySet()
                                      .forEach(
                                          k ->
                                              engine
                                                  .getContext()
                                                  .removeAttribute(k, ScriptContext.ENGINE_SCOPE));
                                  if (right.getClass().isAssignableFrom(Double.class)) {
                                    return (Double) right;
                                  }
                                  return ((Long) right).doubleValue();
                                } catch (ScriptException e) {
                                  throw new VtlRuntimeException(
                                      new VtlScriptException(
                                          "right hierarchical rule has to return long or double",
                                          pos));
                                }
                              });

                  ResolvableExpression expression =
                      ResolvableExpression.withType(Boolean.class)
                          .withPosition(pos)
                          .using(
                              context -> {
                                Bindings bindings = new SimpleBindings(context);
                                bindings.forEach(
                                    (k, v) ->
                                        engine
                                            .getContext()
                                            .setAttribute(k, v, ScriptContext.ENGINE_SCOPE));
                                try {
                                  engine.eval(expressionToEval);
                                  Boolean boolVar =
                                      (Boolean) engine.getContext().getAttribute("bool_var");
                                  engine
                                      .getContext()
                                      .removeAttribute("bool_var", ScriptContext.ENGINE_SCOPE);
                                  bindings
                                      .keySet()
                                      .forEach(
                                          k ->
                                              engine
                                                  .getContext()
                                                  .removeAttribute(k, ScriptContext.ENGINE_SCOPE));
                                  return boolVar;
                                } catch (ScriptException e) {
                                  throw new VtlRuntimeException(
                                      new VtlScriptException(
                                          "hierarchical rule has to return boolean", pos));
                                }
                              });

                  ResolvableExpression errorCodeExpression =
                      null != r.erCode() ? expressionVisitor.visit(r.erCode()) : null;
                  ResolvableExpression errorLevelExpression =
                      null != r.erLevel() ? expressionVisitor.visit(r.erLevel()) : null;
                  return new HierarchicalRule(
                      ruleName,
                      valueDomainValue,
                      expression,
                      leftExpression,
                      rightExpression,
                      codeItems,
                      errorCodeExpression,
                      errorLevelExpression);
                })
            .collect(Collectors.toList());
    HierarchicalRuleset hr = new HierarchicalRuleset(rules, variable, erCodeType, erLevelType);
    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
    bindings.put(rulesetName, hr);
    return hr;
  }
}
