package fr.insee.vtl.prov2;

import fr.insee.vtl.prov.utils.PROV;
import java.util.Map;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

/**
 * Richer IR→RDF view (PR-38 / spec 20260728_02 §1.3): maps {@link ProvGraph} {@code dependsOn}
 * edges to lineage predicates without going through the coarse {@link SdthProgramView}.
 *
 * <ul>
 *   <li>value {@code dependsOn} → {@code sdth:wasDerivedFrom} (blueprint + model-v1)
 *   <li>condition {@code dependsOn} ({@code role=condition}) → {@code prov:used}
 *   <li>expression nodes keep {@code sdth:hasSourceCode} ({@code src})
 * </ul>
 *
 * <p>Does not replace {@link fr.insee.vtl.prov.utils.RDFUtils#buildModel} / SDTH program steps —
 * that compatibility path stays unchanged.
 */
public final class RichRdfView {

  private static final String TREVAS = "http://trevas/";
  private static final String SDTH = "http://rdf-vocabulary.ddialliance.org/sdth#";
  /** Local type for IR expression nodes (not in SDTH). */
  private static final String EXPRESSION_TYPE = TREVAS + "Expression";
  /** Local type for IR scalar assignments (not in SDTH). */
  private static final String SCALAR_TYPE = TREVAS + "Scalar";

  private RichRdfView() {}

  public static Model buildModel(ProvGraph graph) {
    Model model = ModelFactory.createDefaultModel();
    model.setNsPrefix("sdth", SDTH);
    model.setNsPrefix("prov", PROV.getURI());
    model.setNsPrefix("rdfs", RDFS.getURI());
    model.setNsPrefix("trevas", TREVAS);

    Property wasDerivedFrom = model.createProperty(SDTH + "wasDerivedFrom");
    Property hasSourceCode = model.createProperty(SDTH + "hasSourceCode");
    Property hasVariableInstance = model.createProperty(SDTH + "hasVariableInstance");
    Property hasRole = model.createProperty("http://id.making-sense.info/vtl/component/hasRole");
    Property hasType = model.createProperty("http://id.making-sense.info/vtl/component/hasType");

    Resource dataframeType = model.createResource(SDTH + "DataframeInstance");
    Resource variableType = model.createResource(SDTH + "VariableInstance");
    Resource expressionType = model.createResource(EXPRESSION_TYPE);
    Resource scalarType = model.createResource(SCALAR_TYPE);

    Map<String, Map<String, String>> vertices = graph.vertices();
    for (Map.Entry<String, Map<String, String>> entry : vertices.entrySet()) {
      String id = entry.getKey();
      Map<String, String> attrs = entry.getValue();
      String kind = attrs.get("kind");
      if (kind == null) {
        continue;
      }
      switch (kind) {
        case "dataset" -> {
          Resource r = resource(model, "dataset", id);
          r.addProperty(RDF.type, dataframeType);
          r.addProperty(RDFS.label, bindingLabel(id, attrs));
        }
        case "variable" -> {
          Resource r = resource(model, "variable", id);
          r.addProperty(RDF.type, variableType);
          r.addProperty(RDFS.label, componentLabel(id));
          if (attrs.get("role") != null) {
            r.addProperty(hasRole, attrs.get("role"));
          }
          if (attrs.get("type") != null) {
            r.addProperty(hasType, attrs.get("type"));
          }
          String datasetId = attrs.get("dataset");
          if (datasetId != null && vertices.containsKey(datasetId)) {
            resource(model, "dataset", datasetId).addProperty(hasVariableInstance, r);
          }
        }
        case "expression" -> {
          Resource r = resource(model, "expression", id);
          r.addProperty(RDF.type, expressionType);
          String src = attrs.get("src");
          if (src != null) {
            r.addProperty(RDFS.label, src);
            r.addProperty(hasSourceCode, src);
          }
        }
        case "scalar" -> {
          Resource r = resource(model, "scalar", id);
          r.addProperty(RDF.type, scalarType);
          r.addProperty(RDFS.label, bindingLabel(id, attrs));
          if (attrs.get("type") != null) {
            r.addProperty(hasType, attrs.get("type"));
          }
        }
        default -> {
          // ignore unknown kinds
        }
      }
    }

    for (ProvGraph.Edge edge : graph.edges()) {
      Resource from = resourceFor(model, edge.from(), vertices);
      Resource to = resourceFor(model, edge.to(), vertices);
      if (from == null || to == null) {
        continue;
      }
      if ("condition".equals(edge.attrs().get("role"))) {
        from.addProperty(PROV.used, to);
      } else {
        from.addProperty(wasDerivedFrom, to);
      }
    }
    return model;
  }

  private static Resource resourceFor(
      Model model, String id, Map<String, Map<String, String>> vertices) {
    Map<String, String> attrs = vertices.get(id);
    if (attrs == null || attrs.get("kind") == null) {
      return null;
    }
    return switch (attrs.get("kind")) {
      case "dataset" -> resource(model, "dataset", id);
      case "variable" -> resource(model, "variable", id);
      case "expression" -> resource(model, "expression", id);
      case "scalar" -> resource(model, "scalar", id);
      default -> null;
    };
  }

  private static Resource resource(Model model, String kind, String id) {
    return model.createResource(TREVAS + kind + "/" + id);
  }

  private static String bindingLabel(String id, Map<String, String> attrs) {
    int at = id.indexOf('@');
    if (at > 0) {
      return id.substring(0, at);
    }
    return attrs.getOrDefault("src", id);
  }

  private static String componentLabel(String varId) {
    int dot = varId.lastIndexOf('.');
    return dot >= 0 ? varId.substring(dot + 1) : varId;
  }
}
