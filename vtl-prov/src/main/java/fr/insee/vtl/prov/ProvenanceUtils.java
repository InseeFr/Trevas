package fr.insee.vtl.prov;

import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import fr.insee.vtl.prov.model.VTLDataset;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.*;

public class ProvenanceUtils {

    //public static List<Object> toBusinessModel(ProvenanceListener listener) {
    //    // TODO: @nico te graph needs to be refactored. I'll try to fix it before monday.
    //
    //    ArrayList<Object> model = new ArrayList<>();
    //    LinkedHashMap<String, ProvenanceListener.Node> variables = listener.variables;
    //    variables.values().forEach(node -> {
    //        String name = node.name;
    //        Map<String, ProvenanceListener.Node> parents = node.parents;
    //        VTLDataset vtlDataset = new VTLDataset(name);
    //        model.add(vtlDataset);
    //    });
    //    return model;
    //}
//
    //public static void toJSON(ProvenanceListener.Node node) {
//
    //}
//
    //public static void toRDF(ProvenanceListener.Node node) {
//
    //}
}
