package fr.insee.vtl.prov2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedPseudograph;

/**
 * Schemaless directed property graph (spec 20260728_02). Vertices are string ids; node and edge
 * properties are string maps. Every edge is a {@code dependsOn}.
 */
public final class ProvGraph {

  public record Edge(String from, String to, Map<String, String> attrs) {}

  private final DirectedPseudograph<String, DefaultEdge> graph =
      new DirectedPseudograph<>(DefaultEdge.class);
  private final Map<String, Map<String, String>> vertices = new LinkedHashMap<>();
  private final Map<DefaultEdge, Map<String, String>> edgeAttrs = new LinkedHashMap<>();

  public void addVertex(String id, Map<String, String> attrs) {
    graph.addVertex(id);
    vertices.put(id, copy(attrs));
  }

  public void addEdge(String from, String to, Map<String, String> attrs) {
    graph.addVertex(from);
    graph.addVertex(to);
    DefaultEdge edge = graph.addEdge(from, to);
    edgeAttrs.put(edge, copy(attrs));
  }

  public Map<String, Map<String, String>> vertices() {
    return vertices;
  }

  public List<Edge> edges() {
    List<Edge> out = new ArrayList<>();
    for (DefaultEdge edge : graph.edgeSet()) {
      out.add(
          new Edge(
              graph.getEdgeSource(edge),
              graph.getEdgeTarget(edge),
              edgeAttrs.getOrDefault(edge, Map.of())));
    }
    return out;
  }

  private static Map<String, String> copy(Map<String, String> attrs) {
    return new LinkedHashMap<>(attrs);
  }
}
