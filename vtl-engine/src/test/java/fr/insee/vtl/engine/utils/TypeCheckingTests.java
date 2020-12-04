package fr.insee.vtl.engine.utils;

import org.junit.jupiter.api.Test;

import static fr.insee.vtl.engine.utils.TypeChecking.hasSameTypeOrNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TypeCheckingTests {

    @Test
    public void testSameTypeOrNull() {
        assertThat(hasSameTypeOrNull("ok", "ok1")).isTrue();
        assertThat(hasSameTypeOrNull("ok", 1)).isFalse();
        assertThat(hasSameTypeOrNull("ok", null)).isTrue();
    }
}
