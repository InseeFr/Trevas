package fr.insee.vtl.prov2.tests;

import fr.insee.vtl.prov2.ProvGraph;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.nio.dot.DOTImporter;

/**
 * Attribute-level view of a provenance graph: vertex id -> attributes, plus a multiset of (from,
 * to, attributes) edges. This is the shape the corpus assertions compare — deliberately independent
 * of any richer IR class (see specs/20260729_02_work-breakdown.md, PR-1).
 *
 * <p>Both sides of an assertion are built the same way: the golden via {@link #fromDot(Path)}, the
 * extractor output via {@link #addVertex}/{@link #addEdge}. Comparison is set-equality; ordering
 * and comments in the .dot files are irrelevant.
 */
public final class Graph {

  /** One directed dependsOn edge with its annotations (op, role, ...). */
  public record Edge(String from, String to, SortedMap<String, String> attrs) {}

  private final SortedMap<String, SortedMap<String, String>> vertices = new TreeMap<>();
  private final List<Edge> edges = new ArrayList<>();

  public static Graph fromDot(Path dotFile) throws IOException {
    org.jgrapht.Graph<String, DefaultEdge> graph = new DirectedPseudograph<>(DefaultEdge.class);
    Map<String, SortedMap<String, String>> vertexAttrs = new HashMap<>();
    // DefaultEdge equality is identity, so a plain map keyed by edge works for parallel edges.
    Map<DefaultEdge, SortedMap<String, String>> edgeAttrs = new HashMap<>();

    DOTImporter<String, DefaultEdge> importer = new DOTImporter<>();
    importer.setVertexFactory(Function.identity());
    importer.addVertexAttributeConsumer(
        (pair, attr) -> {
          if (!"ID".equals(pair.getSecond())) {
            vertexAttrs
                .computeIfAbsent(pair.getFirst(), k -> new TreeMap<>())
                .put(pair.getSecond(), attr.getValue());
          }
        });
    importer.addEdgeAttributeConsumer(
        (pair, attr) ->
            edgeAttrs
                .computeIfAbsent(pair.getFirst(), k -> new TreeMap<>())
                .put(pair.getSecond(), attr.getValue()));
    try (Reader reader = Files.newBufferedReader(dotFile)) {
      importer.importGraph(graph, reader);
    }

    Graph facts = new Graph();
    for (String v : graph.vertexSet()) {
      facts.addVertex(v, vertexAttrs.getOrDefault(v, new TreeMap<>()));
    }
    for (DefaultEdge e : graph.edgeSet()) {
      facts.addEdge(
          graph.getEdgeSource(e),
          graph.getEdgeTarget(e),
          edgeAttrs.getOrDefault(e, new TreeMap<>()));
    }
    return facts;
  }

  public static Graph from(ProvGraph graph) {
    Graph facts = new Graph();
    graph.vertices().forEach(facts::addVertex);
    for (ProvGraph.Edge edge : graph.edges()) {
      facts.addEdge(edge.from(), edge.to(), edge.attrs());
    }
    return facts;
  }

  public void addVertex(String id, Map<String, String> attrs) {
    vertices.put(id, new TreeMap<>(attrs));
  }

  public void addEdge(String from, String to, Map<String, String> attrs) {
    edges.add(new Edge(from, to, new TreeMap<>(attrs)));
  }

  public SortedMap<String, SortedMap<String, String>> vertices() {
    return vertices;
  }

  public List<Edge> edges() {
    List<Edge> sorted = new ArrayList<>(edges);
    sorted.sort(
        Comparator.comparing(Edge::from)
            .thenComparing(Edge::to)
            .thenComparing(e -> e.attrs().toString()));
    return sorted;
  }

  /**
   * Human-readable differences between this (expected) and the given (actual) graph. Empty means
   * the graphs are identical.
   */
  public List<String> diff(Graph actual) {
    List<String> out = new ArrayList<>();
    vertices.forEach(
        (id, attrs) -> {
          SortedMap<String, String> other = actual.vertices.get(id);
          if (other == null) {
            out.add("missing vertex   " + id + " " + attrs);
          } else if (!attrs.equals(other)) {
            out.add("vertex attrs     " + id + " expected " + attrs + " actual " + other);
          }
        });
    actual.vertices.forEach(
        (id, attrs) -> {
          if (!vertices.containsKey(id)) {
            out.add("unexpected vertex " + id + " " + attrs);
          }
        });
    Map<Edge, Long> expectedEdges = countEdges(this.edges);
    Map<Edge, Long> actualEdges = countEdges(actual.edges);
    expectedEdges.forEach(
        (e, n) -> {
          long m = actualEdges.getOrDefault(e, 0L);
          if (m < n) {
            out.add("missing edge     " + render(e) + (n > 1 ? " (x" + (n - m) + ")" : ""));
          }
        });
    actualEdges.forEach(
        (e, n) -> {
          long m = expectedEdges.getOrDefault(e, 0L);
          if (m < n) {
            out.add("unexpected edge  " + render(e) + (n > 1 ? " (x" + (n - m) + ")" : ""));
          }
        });
    return out;
  }

  private static Map<Edge, Long> countEdges(List<Edge> list) {
    Map<Edge, Long> counts = new HashMap<>();
    list.forEach(e -> counts.merge(e, 1L, Long::sum));
    return counts;
  }

  private static String render(Edge e) {
    return "\"" + e.from() + "\" -> \"" + e.to() + "\" " + e.attrs();
  }
}
