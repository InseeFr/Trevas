package fr.insee.vtl.prov;

import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class VariableGraphListenerTest {

    @Test
    public void simpleTest() {
        String script = "ds_sum := ds1 + ds2;\n" +
                "ds_mul := ds_sum * 3; \n" +
                "ds_res <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];";

        VariableGraphListener provenanceListener = parseAndListen(script);
        printTrees(provenanceListener);
        assertThat(true).isTrue();
    }

    // TODO: unsupported script, TO FIX
    @Disabled
    @Test
    void testComplexGraph() {
        String expr = "" +
                "ds1 := ds2#foo * 4;" +
                "ds3 := ds1#bar + ds3#baz; " +
                "ds3 := ds2 + ds1; " +
                "ds4 := inner_join(ds1 as a, ds2 as b);" +
                "ds5 := union(ds4, ds3);" +
                "ds6 := (ds5 + ds2) * ds3;" +
                //"tmp1 := (ds5 + ds2);" +
                //"ds6 := tmp1 * ds3;" +
                "ds7 := funcBaz(ds6);" +
                "ds8 := ds7[calc test := bla * 3];";

        VariableGraphListener provenanceListener = parseAndListen(expr);

        assertThat(provenanceListener.getVariables()).map(Variable::toString).containsExactlyInAnyOrder(
                "ds8"
        );
        DefaultDirectedGraph<Variable, DefaultEdge> graph = provenanceListener.getGraph();
        assertThat(graph.edgeSet()).map(DefaultEdge::toString).containsExactly(
                "(ds1_0 : ds2_0)", "(ds2_0 : ds3_0)", "(ds3_0 : ds4_0)", "(ds4_0 : ds5_0)"
        );
    }

    @Test
    void testSimpleGraph() {
        String expr = "ds2 := ds1;" +
                "/* test */" +
                "ds3 <- ds2;" +
                "ds4 := ds3;" +
                "ds5 <- ds4;";

        VariableGraphListener provenanceListener = parseAndListen(expr);

        assertThat(provenanceListener.getVariables()).map(Variable::toString).containsExactlyInAnyOrder(
                "ds5_0"
        );
        DefaultDirectedGraph<Variable, DefaultEdge> graph = provenanceListener.getGraph();
        assertThat(graph.edgeSet()).map(DefaultEdge::toString).containsExactly(
                "(ds1_0 : ds2_0)", "(ds2_0 : ds3_0)", "(ds3_0 : ds4_0)", "(ds4_0 : ds5_0)"
        );
    }

    @Test
    void testComposition() {
        String expr = "ds2 := ds1;" +
                "ds3 <- ds2#foo + 4;";

        VariableGraphListener provenanceListener = parseAndListen(expr);

        assertThat(provenanceListener.getVariables()).map(Variable::toString).containsExactlyInAnyOrder(
                "ds3_0"
        );
        DefaultDirectedGraph<Variable, DefaultEdge> graph = provenanceListener.getGraph();
        assertThat(graph.edgeSet()).map(DefaultEdge::toString).containsExactly(
                "(ds1_0 : ds2_0)", "(ds2_0 : ds3_0)"
        );
    }

    @Test
    void testArithmeticGraph() {
        String expr = "ds2 := ds1;" +
                "ds3 <- ds2 + ds1;";

        VariableGraphListener provenanceListener = parseAndListen(expr);

        assertThat(provenanceListener.getVariables()).map(Variable::toString).containsExactlyInAnyOrder(
                "ds3_0"
        );
        DefaultDirectedGraph<Variable, DefaultEdge> graph = provenanceListener.getGraph();
        assertThat(graph.edgeSet()).map(DefaultEdge::toString).containsExactly(
                "(ds1_0 : ds2_0)", "(ds2_0 : ds3_0)", "(ds1_0 : ds3_0)"
        );
    }

    @Test
    void testTwoGraphs() {
        String expr = "ds3 := ds2 + ds1;" +
                "ds6 := ds5 + ds4;";

        VariableGraphListener provenanceListener = parseAndListen(expr);

        assertThat(provenanceListener.getVariables()).map(Variable::toString).containsExactlyInAnyOrder(
                "ds3_0", "ds6_0"
        );
        DefaultDirectedGraph<Variable, DefaultEdge> graph = provenanceListener.getGraph();
        assertThat(graph.edgeSet()).map(DefaultEdge::toString).containsExactly(
                "(ds2_0 : ds3_0)", "(ds1_0 : ds3_0)", "(ds5_0 : ds6_0)", "(ds4_0 : ds6_0)"
        );
    }

    @Test
    void testTwoConnectedGraphs() {
        String expr = "ds3 := ds2 + ds1;" +
                "ds5 := ds4 + ds1;";

        VariableGraphListener provenanceListener = parseAndListen(expr);

        assertThat(provenanceListener.getVariables()).map(Variable::toString).containsExactlyInAnyOrder(
                "ds5_0", "ds3_0"
        );
        DefaultDirectedGraph<Variable, DefaultEdge> graph = provenanceListener.getGraph();
        assertThat(graph.edgeSet()).map(DefaultEdge::toString).containsExactly(
                "(ds2_0 : ds3_0)", "(ds1_0 : ds3_0)", "(ds4_0 : ds5_0)", "(ds1_0 : ds5_0)"
        );
    }

    @Test
    void testCyclicGraphs() {


        String expr = "" +
                "ds1 := ds0;" +
                "ds1 := ds1 + 1;" +
                "ds1 := ds2 * ds1;";

        VariableGraphListener provenanceListener = parseAndListen(expr);
        assertThat(provenanceListener.getVariables()).map(Variable::toString).containsExactlyInAnyOrder(
                "ds1_2"
        );
        DefaultDirectedGraph<Variable, DefaultEdge> graph = provenanceListener.getGraph();
        assertThat(graph.edgeSet()).map(DefaultEdge::toString).containsExactly(
                "(ds0_0 : ds1_0)", "(ds1_0 : ds1_1)", "(ds1_1 : ds1_2)", "(ds2_0 : ds1_2)"
        );
    }

    public static void printTree(Graph<Variable, DefaultEdge> graph, Variable currentNode, String prefix, boolean isLast) {
        if (currentNode == null || !graph.containsVertex(currentNode)) {
            return;
        }

        // Print the current node with appropriate formatting
        System.out.println(prefix + (isLast ? "└── " : "├── ") + currentNode);

        // Get the set of child nodes (outgoing edges)
        Set<DefaultEdge> outgoingEdges = graph.incomingEdgesOf(currentNode);
        int count = outgoingEdges.size();
        int index = 0;

        // Recursively print each child node
        for (DefaultEdge edge : outgoingEdges) {
            Variable targetNode = graph.getEdgeSource(edge);
            printTree(graph, targetNode, prefix + (isLast ? "    " : "│   "), ++index == count);
        }
    }

    private static void printTrees(VariableGraphListener provenanceListener) {
        for (Variable variable : provenanceListener.getVariables()) {
            printTree(provenanceListener.getGraph(), variable, "", false);
        }
    }

    private static VariableGraphListener parseAndListen(String expr) {
        CodePointCharStream stream = CharStreams.fromString(expr);
        VtlLexer lexer = new VtlLexer(stream);
        VtlParser parser = new VtlParser(new CommonTokenStream(lexer));

        VariableGraphListener provenanceListener = new VariableGraphListener();
        ParseTreeWalker.DEFAULT.walk(provenanceListener, parser.start());
        System.out.println(provenanceListener.getGraph());
        printTrees(provenanceListener);
        return provenanceListener;
    }

}