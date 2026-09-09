package fr.insee.vtl.prov.extract;

import java.util.List;
import java.util.Map;

/**
 * Result of visiting a VTL expression before assignment (or anonymous materialization) emits a
 * versioned dataset node.
 *
 * <p>Each variant carries the payload for {@link StructureDeriver} and {@link EdgeLinker}. Prefer
 * exhaustive {@code switch} on the sealed type in those classes so new variants cannot ship
 * half-wired.
 */
sealed interface PendingOp {

  /**
   * Dataset id used as the left/focus operand when chaining clauses ({@code ds[…][…]}). For
   * multi-operand ops, chaining requires materialization first.
   */
  String focusId();

  /** Unary / clause ops with a single source dataset. */
  sealed interface HasSrc extends PendingOp {
    String srcId();

    @Override
    default String focusId() {
      return srcId();
    }
  }

  /** Multi-operand ops ({@code +}, join, set, …). Focus is the first operand. */
  sealed interface HasOperands extends PendingOp {
    List<String> operandIds();

    @Override
    default String focusId() {
      return operandIds().get(0);
    }
  }

  /** A resolved dataset reference ({@code ds1}) — no operator pending. */
  record Identity(String datasetId) implements PendingOp {
    @Override
    public String focusId() {
      return datasetId;
    }
  }

  /**
   * Component-wise dataset op: arithmetic ({@code +}, {@code *}), comparisons, boolean, {@code if},
   * and §5.13 functions ({@code abs}, {@code cast}, …). Scalar operands are omitted from {@code
   * operandIds}.
   */
  record ComponentWise(String op, List<String> operandIds) implements HasOperands {}

  record Calc(String srcId, Map<String, String> exprs, Map<String, Class<?>> types)
      implements HasSrc {}

  record Aggr(
      String srcId,
      Map<String, String> exprs,
      Map<String, Class<?>> types,
      List<String> groupBy,
      List<String> havingExprIds)
      implements HasSrc {}

  /**
   * Row filter / subspace ({@code filter}, {@code sub}): structure pass-through plus condition
   * expression nodes. {@code op} is the edge annotation ({@code filter} / {@code sub}).
   */
  record ConditionClause(String op, String srcId, List<String> conditionExprIds)
      implements HasSrc {}

  record Keep(String srcId, List<String> columns) implements HasSrc {}

  record Drop(String srcId, List<String> columns) implements HasSrc {}

  /** {@code renameFrom}: output name → input name. */
  record Rename(String srcId, Map<String, String> renameFrom) implements HasSrc {}

  /** Empty-body join; {@code op} is the keyword ({@code inner_join}, …). */
  record Join(String op, List<String> operandIds) implements HasOperands {}

  /**
   * Set operator; {@code op} is {@code union}/{@code intersect}/{@code setdiff}/{@code symdiff}.
   */
  record SetOp(String op, List<String> operandIds) implements HasOperands {}

  /**
   * {@code check_datapoint(ds, ruleset …)}. {@code validatedVars} come from the datapoint ruleset
   * signature ({@code variable …}).
   */
  record CheckDatapoint(String srcId, String ruleset, List<String> validatedVars)
      implements HasSrc {}

  /**
   * {@code check_hierarchy(ds, ruleset …)}. Same validation-column shape as {@link CheckDatapoint};
   * {@code validatedVars} are the RULE component (if present) or source measures.
   */
  record CheckHierarchy(String srcId, String ruleset, List<String> validatedVars)
      implements HasSrc {}

  /**
   * {@code check(ds … [imbalance imb] …)}. {@code imbalanceId} is null when the clause is omitted.
   */
  record Check(String srcId, String imbalanceId) implements HasSrc {}

  /**
   * Unary producer that keeps the operand structure: {@code hierarchy}, time-series. Optional
   * {@code ruleset} annotates dataset and measure edges.
   */
  record PassThrough(String srcId, String op, String ruleset) implements HasSrc {
    PassThrough(String srcId, String op) {
      this(srcId, op, null);
    }
  }

  /**
   * {@code exists_in(left, right)}: left identifiers + {@code bool_var}; right is membership {@code
   * role=condition}.
   */
  record ExistsIn(String leftId, String rightId) implements PendingOp {
    @Override
    public String focusId() {
      return leftId;
    }
  }

  /**
   * {@code ds[pivot id, measure]} or {@code ds[customPivot …]}. {@code op} is {@code pivot} or
   * {@code customPivot}.
   */
  record Pivot(
      String srcId,
      String idComponent,
      String measureComponent,
      List<String> pivotedColumns,
      String op)
      implements HasSrc {}

  /** {@code ds[unpivot id, measure]} — inverse of pivot. */
  record Unpivot(String srcId, String idComponent, String measureComponent) implements HasSrc {}

  /** {@code ds#component} — identifiers + selected component. */
  record Membership(String srcId, String component) implements HasSrc {}

  /**
   * Join {@code apply} body: identifiers kept; measures replaced by a single default-named measure.
   */
  record Apply(String srcId, String exprId, String measureName, Class<?> measureType)
      implements HasSrc {}

  /**
   * Dataset-level analytic ({@code sum(ds over …)}). Structure equals the operand; partition/order
   * are condition expression nodes.
   */
  record Analytic(String srcId, String op, List<String> conditionExprIds) implements HasSrc {}

  /**
   * External black-box ({@code eval}) with zero or more dataset operands. Empty → empty structure.
   */
  record External(String op, List<String> operandIds) implements PendingOp {
    @Override
    public String focusId() {
      return operandIds.isEmpty() ? "" : operandIds.get(0);
    }
  }
}
