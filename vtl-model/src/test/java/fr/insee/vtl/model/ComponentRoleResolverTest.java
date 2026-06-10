package fr.insee.vtl.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ComponentRoleResolverTest {

  @Test
  void parsesEnumRoleNames() {
    assertThat(ComponentRoleResolver.parseRoleName("MEASURE")).isEqualTo(Dataset.Role.MEASURE);
    assertThat(ComponentRoleResolver.parseRoleName("ATTRIBUTE")).isEqualTo(Dataset.Role.ATTRIBUTE);
    assertThat(ComponentRoleResolver.parseRoleName("IDENTIFIER"))
        .isEqualTo(Dataset.Role.IDENTIFIER);
    assertThat(ComponentRoleResolver.parseRoleName("VIRALATTRIBUTE"))
        .isEqualTo(Dataset.Role.VIRALATTRIBUTE);
  }

  @Test
  void parsesViralAttributeAliases() {
    assertThat(ComponentRoleResolver.parseRoleName("viral attribute"))
        .isEqualTo(Dataset.Role.VIRALATTRIBUTE);
    assertThat(ComponentRoleResolver.parseRoleName("VIRAL_ATTRIBUTE"))
        .isEqualTo(Dataset.Role.VIRALATTRIBUTE);
  }

  @Test
  void mapsDimensionAndComponentToIdentifier() {
    assertThat(ComponentRoleResolver.parseRoleName("DIMENSION")).isEqualTo(Dataset.Role.IDENTIFIER);
    assertThat(ComponentRoleResolver.parseRoleName("component")).isEqualTo(Dataset.Role.IDENTIFIER);
  }

  @Test
  void rejectsBlankRole() {
    assertThatThrownBy(() -> ComponentRoleResolver.parseRoleName("  "))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
