package fr.insee.vtl.sdmx;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class BPETest {

    private SparkSession spark;
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");
        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @Test
    public void buildDataset() throws ScriptException {
        Utils u = new Utils();
        Structured.DataStructure structure = u.buildStructureFromSDMX3("src/test/resources/DSD_BPE_DETAIL.xml", "BPE_DETAIL");

        SparkDataset bpeDetailDs = new SparkDataset(
                spark.read()
                        .option("header", "true")
                        .option("delimiter", ";")
                        .option("quote", "\"")
                        .csv("src/test/resources/BPE_DETAIL_SAMPLE.csv"),
                structure
        );

        assertThat(bpeDetailDs.getDataStructure().size()).isEqualTo(6);

        ScriptContext context = engine.getContext();
        context.setAttribute("BPE_DETAIL", bpeDetailDs, ScriptContext.ENGINE_SCOPE);

        // Step 1
        engine.eval("" +
                "define datapoint ruleset UNIQUE_MUNICIPALITY (variable DEPCOM) is\n" +
                "    MUNICIPALITY_FORMAT_RULE : match_characters(DEPCOM, \"[0-9]{5}|2[A-B][0-9]{3}\") errorcode \"Municipality code is not in the correct format\"\n" +
                "end datapoint ruleset;\n" +
                "\n" +
                "CHECK_MUNICIPALITY := check_datapoint(BPE_DETAIL, UNIQUE_MUNICIPALITY invalid);");

        Dataset checkMunicipality = (Dataset) engine.getContext().getAttribute("CHECK_MUNICIPALITY");

        assertThat(checkMunicipality.getDataPoints()).isEmpty();

        // Step 2
        engine.eval("BPE_DETAIL_CLEAN := BPE_DETAIL" +
                "   [drop LAMBERT_X, LAMBERT_Y]\n" +
                "   [rename ID_EQUIPEMENT to id, TYPEQU to facility_type, DEPCOM to municipality, REF_YEAR to year];");

        Dataset bpeDetailClean = (Dataset) engine.getContext().getAttribute("BPE_DETAIL_CLEAN");
        Structured.DataStructure bpeDetailCleanStructure = bpeDetailClean.getDataStructure();

        assertThat(bpeDetailCleanStructure.get("id").getType()).isEqualTo(String.class);
        assertThat(bpeDetailCleanStructure.get("id").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(bpeDetailCleanStructure.get("facility_type").getType()).isEqualTo(String.class);
        assertThat(bpeDetailCleanStructure.get("facility_type").getRole()).isEqualTo(Dataset.Role.ATTRIBUTE);

        assertThat(bpeDetailCleanStructure.get("municipality").getType()).isEqualTo(String.class);
        assertThat(bpeDetailCleanStructure.get("municipality").getRole()).isEqualTo(Dataset.Role.ATTRIBUTE);

        assertThat(bpeDetailCleanStructure.get("year").getType()).isEqualTo(String.class);
        assertThat(bpeDetailCleanStructure.get("year").getRole()).isEqualTo(Dataset.Role.ATTRIBUTE);

        // Step 3
        engine.eval("BPE_MUNICIPALITY <- BPE_DETAIL_CLEAN" +
                "   [aggr nb := count(id) group by municipality, year, facility_type];");

        Dataset bpeMunicipality = (Dataset) engine.getContext().getAttribute("BPE_MUNICIPALITY");
        Structured.DataStructure bpeMunicipalityStructure = bpeMunicipality.getDataStructure();

        assertThat(bpeMunicipalityStructure.get("municipality").getType()).isEqualTo(String.class);
        assertThat(bpeMunicipalityStructure.get("municipality").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(bpeMunicipalityStructure.get("facility_type").getType()).isEqualTo(String.class);
        assertThat(bpeMunicipalityStructure.get("facility_type").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(bpeMunicipalityStructure.get("year").getType()).isEqualTo(String.class);
        assertThat(bpeMunicipalityStructure.get("year").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);


        assertThat(bpeMunicipalityStructure.get("nb").getType()).isEqualTo(Long.class);
        assertThat(bpeMunicipalityStructure.get("nb").getRole()).isEqualTo(Dataset.Role.MEASURE);

    }
}
