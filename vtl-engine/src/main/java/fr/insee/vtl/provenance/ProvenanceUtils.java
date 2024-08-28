package fr.insee.vtl.provenance;

import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import fr.insee.vtl.provenance.model.VTLDataset;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.*;

public class ProvenanceUtils {

    public static ProvenanceListener getProvenance(String script) {
        CodePointCharStream stream = CharStreams.fromString(script);
        VtlLexer lexer = new VtlLexer(stream);
        VtlParser parser = new VtlParser(new CommonTokenStream(lexer));

        ProvenanceListener provenanceListener = new ProvenanceListener();
        ParseTreeWalker.DEFAULT.walk(provenanceListener, parser.start());

        return provenanceListener;
    }

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

    public static List<Object> toBusinessModel(ProvenanceListener listener) {
        ArrayList<Object> model = new ArrayList<>();
        LinkedHashMap<String, ProvenanceListener.Node> variables = listener.variables;
        variables.values().forEach(node -> {
            String name = node.name;
            Map<String, ProvenanceListener.Node> parents = node.parents;
            VTLDataset vtlDataset = new VTLDataset(name);
            model.add(vtlDataset);
        });
        return model;
    }

    public static void toJSON(ProvenanceListener.Node node) {

    }

    public static void toRDF(ProvenanceListener.Node node) {

    }
}
