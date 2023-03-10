package fr.insee.vtl.model;

public interface Positioned {

    class Position {
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

    Position getPosition();
}
