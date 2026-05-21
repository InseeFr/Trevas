package fr.insee.vtl.engine.attribute;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AttributePropagationStructureTest {

  @Test
  void mergeStructureUnionsViralAttributesOnly() {
    Structured.DataStructure left =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("At_plain", String.class, Dataset.Role.ATTRIBUTE),
                new Structured.Component("At_viral", String.class, Dataset.Role.VIRALATTRIBUTE)));

    Structured.DataStructure right =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_2", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("At_other", String.class, Dataset.Role.VIRALATTRIBUTE)));

    Structured.DataStructure merged = AttributePropagation.mergeStructure(left, right);

    assertThat(merged.get("Id_1")).isNotNull();
    assertThat(merged.get("Me_1")).isNotNull();
    assertThat(merged.get("Me_2")).isNotNull();
    assertThat(merged.get("At_plain")).isNull();
    assertThat(merged.get("At_viral").getRole()).isEqualTo(Dataset.Role.VIRALATTRIBUTE);
    assertThat(merged.get("At_other").getRole()).isEqualTo(Dataset.Role.VIRALATTRIBUTE);
  }

  @Test
  void unaryStructureKeepsIdentifiersMeasuresAndViralAttributes() {
    Structured.DataStructure input =
        new Structured.DataStructure(
            List.of(
                new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("At_1", String.class, Dataset.Role.ATTRIBUTE),
                new Structured.Component("At_2", String.class, Dataset.Role.VIRALATTRIBUTE)));

    Structured.DataStructure result =
        AttributePropagation.unaryStructure(input, Map.of("Me_2", Double.class));

    assertThat(result.get("Id_1")).isNotNull();
    assertThat(result.get("Me_2").getRole()).isEqualTo(Dataset.Role.MEASURE);
    assertThat(result.get("Me_1")).isNull();
    assertThat(result.get("At_1")).isNull();
    assertThat(result.get("At_2").getRole()).isEqualTo(Dataset.Role.VIRALATTRIBUTE);
  }
}
