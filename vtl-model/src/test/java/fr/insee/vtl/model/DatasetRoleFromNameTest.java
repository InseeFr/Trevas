package fr.insee.vtl.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class DatasetRoleFromNameTest {

  @Test
  void parsesEnumRoleNames() {
    assertThat(Dataset.Role.fromName("MEASURE")).isEqualTo(Dataset.Role.MEASURE);
    assertThat(Dataset.Role.fromName("ATTRIBUTE")).isEqualTo(Dataset.Role.ATTRIBUTE);
    assertThat(Dataset.Role.fromName("IDENTIFIER")).isEqualTo(Dataset.Role.IDENTIFIER);
    assertThat(Dataset.Role.fromName("VIRALATTRIBUTE")).isEqualTo(Dataset.Role.VIRALATTRIBUTE);
  }

  @Test
  void normalizesWhitespaceAndCase() {
    assertThat(Dataset.Role.fromName("viral attribute")).isEqualTo(Dataset.Role.VIRALATTRIBUTE);
    assertThat(Dataset.Role.fromName("  measure ")).isEqualTo(Dataset.Role.MEASURE);
  }

  @Test
  void rejectsBlankRole() {
    assertThatThrownBy(() -> Dataset.Role.fromName("  "))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
