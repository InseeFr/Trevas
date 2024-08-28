package fr.insee.vtl.provenance;

import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Collection;


public class ProvenanceListenerTest {

    public static void printTree(ProvenanceListener.Node node, String prefix, boolean isLast) {
        if (node == null) {
            return;
        }

        // Print the current node with appropriate formatting
        System.out.println(prefix + (isLast ? "└── " : "├── ") + node.name + "\t[ " + node.expression + " ]");

        // Get the list of parent nodes
        Collection<ProvenanceListener.Node> parents = node.parents.values();
        int count = parents.size();
        int index = 0;

        // Recursively print each parent node
        for (ProvenanceListener.Node parent : parents) {
            printTree(parent, prefix + (isLast ? "    " : "│   "), ++index == count);
        }
    }

    @Test
    void testProvenance(TestInfo testInfo) {
        String expr;

        expr = "" +
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


        // entity(e1, [ prov:type="prov:Entity", prov:label="data.csv" ])
        // activity(a1, [ prov:type="prov:Activity", prov:label="process_data", prov:startTime="2024-08-28T09:00:00", prov:endTime="2024-08-28T09:30:00" ])
        // entity(ds2, [ prov:type="prov:Entity", prov:label="ds2" ])
        // activity(ds1, [prov:type="prov:Activity", prov:label="ds2#foo * 4", ])
        // entity(ds1, [ prov:type="prov:Entity", prov:label="ds1" ])
        // wasDerivedFrom(ds1, ds2, ds1)

        CodePointCharStream stream = CharStreams.fromString(expr, testInfo.getTestMethod().get().getName());
        VtlLexer lexer = new VtlLexer(stream);
        VtlParser parser = new VtlParser(new CommonTokenStream(lexer));

        ProvenanceListener provenanceListener = new ProvenanceListener();
        ParseTreeWalker.DEFAULT.walk(provenanceListener, parser.start());

        // Edge [ { a } { b } ....]
        // Vertices [ [a, b] [a, c]



        // Node { a, parent: [ Node { b :

        System.out.println(provenanceListener.variables.keySet());

        printTree(provenanceListener.variables.get("ds8"), "", true);

    }
}