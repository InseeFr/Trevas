package fr.insee.vtl.prov;

import org.junit.jupiter.api.Test;


public class ProvenanceListenerTest {

    @Test
    void testProvenance() {
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


        // TODO: DFS
        // TODO: Tarjan / Kosaraju ?
        // TODO: Non-cyclical.
        // https://memgraph.com/blog/graph-algorithms-applications
        // https://chatgpt.com/c/9d76bc9b-35cb-4c9a-a126-180aa1678192
        // https://chatgpt.com/c/6f306284-8130-4583-bbc3-8ea67b33ac6c
        // Edge [ { a } { b } ....]
        // Vertices [ [a, b] [a, c]


        // Node { a, parent: [ Node { b :
        ProvenanceListener provListener = ProvenanceUtils.getProvenance(expr);
        System.out.println(provListener.variables.keySet());
        ProvenanceUtils.toBusinessModel(provListener);
        ProvenanceUtils.printTree(provListener.variables.get("ds8"), "", true);

    }
}