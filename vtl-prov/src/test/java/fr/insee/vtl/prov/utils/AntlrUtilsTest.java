package fr.insee.vtl.prov.utils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class AntlrUtilsTest {

    String simpleScript =
            """
                          ds_sum := ds1 + ds2;
                          ds_mul := ds_sum * 3;\s
                          ds_res <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];\
                    """;

    String simpleValidation =
            """
                          define datapoint ruleset test (variable sex) is
                              myrule : sex in {"M"} errorcode "ERROR"
                          end datapoint ruleset;
                          pengfei.ds_result <- check_datapoint(pengfei.pengfei, test);
                    """;

    @Test
    public void getDefineStatementsTest() {
        assertThat(AntlrUtils.getDefineStatements(simpleScript).size()).isEqualTo(0);

        assertThat(AntlrUtils.getDefineStatements(simpleValidation).size()).isEqualTo(1);
        assertThat(AntlrUtils.getDefineStatements(simpleValidation).get("test"))
                .contains("define datapoint ruleset test (variable sex) is");
    }

    @Test
    public void getAssignmentStatementsTest() {
        List<String> simpleScriptAssignmentStatements =
                AntlrUtils.getAssignmentStatements(simpleScript);
        assertThat(simpleScriptAssignmentStatements.size()).isEqualTo(3);
        assertThat(simpleScriptAssignmentStatements.get(0)).isEqualTo("ds_sum := ds1 + ds2;");

        assertThat(AntlrUtils.getAssignmentStatements(simpleValidation).size()).isEqualTo(1);
    }
}
