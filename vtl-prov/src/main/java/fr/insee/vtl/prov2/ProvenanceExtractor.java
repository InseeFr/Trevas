package fr.insee.vtl.prov2;

import fr.insee.vtl.antlr.runtime.CharStreams;
import fr.insee.vtl.antlr.runtime.CommonTokenStream;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import java.util.List;

/**
 * Provenance entry point: parse → grammar support check ({@link SupportCheckVisitor}) → structure
 * oracle → {@link ProvenanceVisitor} ({@code VtlBaseVisitor<Void>}) mutating a shared {@link
 * ProvGraph}. The visitor’s per-expression state is a sealed {@link PendingOp}.
 *
 * <p>Must throw {@link UnsupportedOperationException} with an {@code unsupported: …} message on
 * syntax not yet handled — never a plausible-but-wrong graph.
 */
public final class ProvenanceExtractor {

  public ProvGraph extract(String script, List<InputDataset> inputs) {
    VtlParser.StartContext start = parse(script);
    new SupportCheckVisitor().visit(start);
    StructureOracle oracle = StructureOracle.run(script, inputs);
    ProvGraph graph = new ProvGraph();
    new ProvenanceVisitor(graph, oracle, inputs).visit(start);
    return graph;
  }

  private static VtlParser.StartContext parse(String script) {
    VtlLexer lexer = new VtlLexer(CharStreams.fromString(script));
    return new VtlParser(new CommonTokenStream(lexer)).start();
  }
}
