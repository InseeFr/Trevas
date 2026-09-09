package fr.insee.vtl.prov.extract;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import fr.insee.vtl.prov.ir.ProvGraph;
import fr.insee.vtl.testutils.InputDataset;
import java.util.List;
import javax.script.ScriptContext;

/**
 * Provenance entry point: parse → grammar support check ({@link SupportCheckVisitor}) → structure
 * oracle → {@link ProvenanceVisitor} ({@code VtlBaseVisitor<Void>}) mutating a shared {@link
 * ProvGraph}. The visitor’s per-expression state is a sealed {@link PendingOp}; structure
 * derivation and edge linking live in {@link StructureDeriver} / {@link EdgeLinker}.
 *
 * <p>Must throw {@link UnsupportedOperationException} with an {@code unsupported: …} message on
 * syntax not yet handled — never a plausible-but-wrong graph.
 */
public final class ProvenanceExtractor {

  /**
   * Extract provenance: runs a dedicated structure-oracle eval. Prefer {@link
   * #extractFromEvaluatedContext} when the caller already evaluated the script.
   */
  public ProvGraph extract(String script, List<InputDataset> inputs) {
    return extract(script, inputs, StructureOracle.run(script, inputs));
  }

  /**
   * Extract provenance reusing bindings from a context that already evaluated {@code script}
   * (avoids a second {@code engine.eval}).
   */
  public ProvGraph extractFromEvaluatedContext(
      String script, List<InputDataset> inputs, ScriptContext context) {
    return extract(script, inputs, StructureOracle.fromContext(context, true));
  }

  private ProvGraph extract(String script, List<InputDataset> inputs, StructureOracle oracle) {
    VtlParser.StartContext start = parse(script);
    ScriptSymbols symbols = new ScriptSymbols();
    new SupportCheckVisitor(symbols).visit(start);
    ProvGraph graph = new ProvGraph();
    new ProvenanceVisitor(graph, oracle, inputs, symbols).visit(start);
    return graph;
  }

  private static VtlParser.StartContext parse(String script) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(script));
    return new VtlParser(new CommonTokenStream(lexer)).start();
  }
}
