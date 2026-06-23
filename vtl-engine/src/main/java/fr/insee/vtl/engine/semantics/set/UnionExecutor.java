package fr.insee.vtl.engine.semantics.set;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.antlr.runtime.RuleContext;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;
import java.util.stream.Collectors;

/** VTL set-operator orchestration ({@code union}, etc.). */
public final class UnionExecutor {

  private UnionExecutor() {}

  public static DatasetExpression union(
      ProcessingEngine engine, List<DatasetExpression> datasets, VtlParser.UnionAtomContext ctx) {
    Structured.DataStructure structure = null;
    for (int i = 0; i < datasets.size(); i++) {
      DatasetExpression dataset = datasets.get(i);
      if (structure == null) {
        structure = dataset.getDataStructure();
      } else if (!structure.equals(dataset.getDataStructure())) {
        VtlParser.ExprContext expr = ctx.expr(i);
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "dataset structure of %s is incompatible with %s"
                    .formatted(
                        expr.getText(),
                        ctx.expr().stream()
                            .map(RuleContext::getText)
                            .collect(Collectors.joining(", "))),
                fromContext(ctx)));
      }
    }
    List<String> dedupeOn =
        datasets.get(0).getIdentifiers().stream().map(Structured.Component::getName).toList();
    return engine.executeUnion(datasets, dedupeOn);
  }
}
