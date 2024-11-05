package fr.insee.vtl.prov.utils;

import java.util.Map;
import java.util.UUID;

public class ProvenanceUtils {

    public static String getOrBuildUUID(Map<String, String> availableDataframeUUID, String label) {
        if (null != availableDataframeUUID.get(label)) {
            return availableDataframeUUID.get(label);
        }
        return UUID.randomUUID().toString();
    }

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
