package fr.insee.vtl.model;

import java.io.Serializable;
import java.util.Comparator;

public interface Positioned {

  Position getPosition();

  record Position(Integer startLine, Integer endLine, Integer startColumn, Integer endColumn)
      implements Serializable, Comparable<Position> {
    @Override
    public int compareTo(Position other) {
      return Comparator.nullsLast(
              Comparator.comparing(Position::startLine)
                  .thenComparing(Position::endLine)
                  .thenComparing(Position::startColumn)
                  .thenComparing(Position::endColumn))
          .compare(this, other);
    }
  }
}
