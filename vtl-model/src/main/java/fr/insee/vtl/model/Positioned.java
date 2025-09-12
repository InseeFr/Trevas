package fr.insee.vtl.model;

import java.io.Serializable;

public interface Positioned {

  Position getPosition();

  record Position(Integer startLine, Integer endLine, Integer startColumn, Integer endColumn)
      implements Serializable, Comparable<Position> {
    @Override
    public int compareTo(Position other) {
      if (this.startLine != null && other.startLine != null) {
        int cmp = this.startLine.compareTo(other.startLine);
        if (cmp != 0) return cmp;
      }

      if (this.endLine != null && other.endLine != null) {
        int cmp = this.endLine.compareTo(other.endLine);
        if (cmp != 0) return cmp;
      }

      if (this.startColumn != null && other.startColumn != null) {
        int cmp = this.startColumn.compareTo(other.startColumn);
        if (cmp != 0) return cmp;
      }

      if (this.endColumn != null && other.endColumn != null) {
        return this.endColumn.compareTo(other.endColumn);
      }

      return 0;
    }
  }
}
