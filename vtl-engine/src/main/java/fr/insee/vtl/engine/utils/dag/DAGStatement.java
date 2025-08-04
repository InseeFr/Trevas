package fr.insee.vtl.engine.utils.dag;

import java.util.Set;

/**
 * Representation of a VTL Statement
 *
 * @param unsortedIndex index of the original parsed statement without resorting
 * @param produces Produced data
 * @param consumes Consumed data
 */
public record DAGStatement(int unsortedIndex, String produces, Set<String> consumes) {

  @Override
  public String toString() {
    return "Statement{"
        + "unsortedIndex='"
        + unsortedIndex
        + '\''
        + ", produces="
        + produces
        + ", consumes="
        + consumes
        + '}';
  }
}
