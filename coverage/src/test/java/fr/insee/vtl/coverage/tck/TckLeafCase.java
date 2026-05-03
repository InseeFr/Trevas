package fr.insee.vtl.coverage.tck;

import fr.insee.vtl.coverage.model.Test;
import java.util.Objects;

/** One executable TCK scenario: Trevas display path + loaded payload. */
public record TckLeafCase(String displayPath, Test payload) {
  public TckLeafCase {
    Objects.requireNonNull(payload, "payload");
    Objects.requireNonNull(displayPath, "displayPath");
  }
}
