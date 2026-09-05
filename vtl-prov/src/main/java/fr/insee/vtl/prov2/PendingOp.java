package fr.insee.vtl.prov2;

import java.util.List;
import java.util.Map;

/**
 * Result of visiting a VTL expression before assignment (or anonymous materialization) emits a
 * versioned dataset node.
 *
 * <p>Replaces the previous bag of {@code lastOp} / {@code lastCalcExprs} / … fields: each variant
 * carries exactly the data needed to derive its structure and link provenance edges.
 */
sealed interface PendingOp {

  /**
   * Dataset id used as the left/focus operand when chaining clauses ({@code ds[…][…]}): for an
   * identity this is the dataset itself; for a unary clause it is the source; for multi-operand
   * ops it is unused (chaining requires materialization first, which only applies to unary
   * clauses).
   */
  String focusId();

  /** A resolved dataset reference ({@code ds1}) — no operator pending. */
  record Identity(String datasetId) implements PendingOp {
    @Override
    public String focusId() {
      return datasetId;
    }
  }

  /** Dataset arithmetic ({@code +}, {@code *}, …), possibly with a scalar operand omitted. */
  record Arithmetic(String op, List<String> operandIds) implements PendingOp {
    @Override
    public String focusId() {
      return operandIds.get(0);
    }
  }

  record Calc(String srcId, Map<String, String> exprs, Map<String, Class<?>> types)
      implements PendingOp {
    @Override
    public String focusId() {
      return srcId;
    }
  }

  record Aggr(
      String srcId,
      Map<String, String> exprs,
      Map<String, Class<?>> types,
      List<String> groupBy)
      implements PendingOp {
    @Override
    public String focusId() {
      return srcId;
    }
  }

  record Filter(String srcId, List<String> conditionExprIds) implements PendingOp {
    @Override
    public String focusId() {
      return srcId;
    }
  }

  record Sub(String srcId, List<String> conditionExprIds) implements PendingOp {
    @Override
    public String focusId() {
      return srcId;
    }
  }

  record Keep(String srcId, List<String> columns) implements PendingOp {
    @Override
    public String focusId() {
      return srcId;
    }
  }

  record Drop(String srcId, List<String> columns) implements PendingOp {
    @Override
    public String focusId() {
      return srcId;
    }
  }

  /** {@code renameFrom}: output name → input name. */
  record Rename(String srcId, Map<String, String> renameFrom) implements PendingOp {
    @Override
    public String focusId() {
      return srcId;
    }
  }

  /** Empty-body join; {@code op} is the keyword ({@code inner_join}, …). */
  record Join(String op, List<String> operandIds) implements PendingOp {
    @Override
    public String focusId() {
      return operandIds.get(0);
    }
  }

  /**
   * Set operator; {@code op} is {@code union}/{@code intersect}/{@code setdiff}/{@code symdiff}.
   */
  record SetOp(String op, List<String> operandIds) implements PendingOp {
    @Override
    public String focusId() {
      return operandIds.get(0);
    }
  }
}
