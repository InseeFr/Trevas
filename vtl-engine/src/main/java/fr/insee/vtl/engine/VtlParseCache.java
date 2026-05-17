package fr.insee.vtl.engine;

import fr.insee.vtl.parser.VtlParser;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/** LRU cache of ANTLR parse trees keyed by script source text. */
final class VtlParseCache {

  private static final int MAX_ENTRIES = 64;

  private final Map<String, VtlParser.StartContext> entries =
      new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, VtlParser.StartContext> eldest) {
          return size() > MAX_ENTRIES;
        }
      };

  VtlParser.StartContext get(String script, Function<String, VtlParser.StartContext> parser) {
    synchronized (entries) {
      return entries.computeIfAbsent(script, parser);
    }
  }

  void clear() {
    synchronized (entries) {
      entries.clear();
    }
  }

  int size() {
    synchronized (entries) {
      return entries.size();
    }
  }
}
