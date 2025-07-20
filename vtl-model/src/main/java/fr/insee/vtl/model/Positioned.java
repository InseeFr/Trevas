package fr.insee.vtl.model;

import java.io.Serializable;

public interface Positioned {

  Position getPosition();

  class Position implements Serializable {
    public final Integer startLine;
    public final Integer endLine;
    public final Integer startColumn;
    public final Integer endColumn;

    public Position(Integer startLine, Integer endLine, Integer startColumn, Integer endColumn) {
      this.startLine = startLine;
      this.endLine = endLine;
      this.startColumn = startColumn;
      this.endColumn = endColumn;
    }
  }
}
