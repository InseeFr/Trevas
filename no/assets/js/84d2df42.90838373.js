"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[9699],{17646:e=>{e.exports=JSON.parse('{"blogPosts":[{"id":"/trevas-java-17","metadata":{"permalink":"/Trevas/no/blog/trevas-java-17","source":"@site/blog/2023-11-22-trevas-java-17.mdx","title":"Trevas - Java 17","description":"News","date":"2023-11-22T00:00:00.000Z","formattedDate":"22. november 2023","tags":[{"label":"Trevas","permalink":"/Trevas/no/blog/tags/trevas"}],"readingTime":0.345,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-java-17","title":"Trevas - Java 17","authors":["nicolas"],"tags":["Trevas"]},"nextItem":{"title":"Trevas - Persistent assignments","permalink":"/Trevas/no/blog/trevas-persistent-assignments"}},"content":"### News\\n\\nTrevas 1.2.0 enables Java 17 support.\\n\\n### Java modules handling\\n\\nSpark does not support Java modules.\\n\\nJava 17 client apps, embedding Trevas in Spark mode have to configure `UNNAMED` modules for Spark.\\n\\n#### Maven\\n\\nAdd to your `pom.xml` file, in the `build > plugins` section:\\n\\n```xml\\n<plugin>\\n    <groupId>org.apache.maven.plugins</groupId>\\n    <artifactId>maven-compiler-plugin</artifactId>\\n    <version>3.11.0</version>\\n    <configuration>\\n        <compilerArgs>\\n            <arg>--add-exports</arg>\\n            <arg>java.base/sun.nio.ch=ALL-UNNAMED</arg>\\n        </compilerArgs>\\n    </configuration>\\n</plugin>\\n```\\n\\n#### Docker\\n\\n```shell\\nENTRYPOINT [\\"java\\", \\"--add-exports\\", \\"java.base/sun.nio.ch=ALL-UNNAMED\\", \\"mainClass\\"]\\n```"},{"id":"/trevas-persistent-assignments","metadata":{"permalink":"/Trevas/no/blog/trevas-persistent-assignments","source":"@site/blog/2023-11-22-trevas-persistent-assignment.mdx","title":"Trevas - Persistent assignments","description":"News","date":"2023-11-22T00:00:00.000Z","formattedDate":"22. november 2023","tags":[{"label":"Trevas","permalink":"/Trevas/no/blog/tags/trevas"}],"readingTime":0.41,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-persistent-assignments","title":"Trevas - Persistent assignments","authors":["nicolas"],"tags":["Trevas"]},"prevItem":{"title":"Trevas - Java 17","permalink":"/Trevas/no/blog/trevas-java-17"},"nextItem":{"title":"Trevas - check_hierarchy","permalink":"/Trevas/no/blog/trevas-check_hierarchy"}},"content":"### News\\n\\nTrevas 1.2.0 includes the persistent assignment support: `ds1 <- ds;`.\\n\\nIn Trevas, persistent datasets are represented as `PersistentDataset`.\\n\\n### Handle `PersistentDataset`\\n\\nTrevas datasets are represented as `Dataset`.\\n\\nAfter running the Trevas engine, you can use persistent datasets with something like:\\n\\n```\\nBindings engineBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);\\nengineBindings.forEach((k, v) -> {\\n    if (v instanceof PersistentDataset) {\\n        fr.insee.vtl.model.Dataset ds = ((PersistentDataset) v).getDelegate();\\n        if (ds instanceof SparkDataset) {\\n            Dataset<Row> sparkDs = ((SparkDataset) ds).getSparkDataset();\\n            // Do what you want with sparkDs\\n        }\\n    }\\n});\\n```"},{"id":"/trevas-check_hierarchy","metadata":{"permalink":"/Trevas/no/blog/trevas-check_hierarchy","source":"@site/blog/2023-09-01-trevas-check_hierarchy.mdx","title":"Trevas - check_hierarchy","description":"News","date":"2023-09-01T00:00:00.000Z","formattedDate":"1. september 2023","tags":[{"label":"Trevas","permalink":"/Trevas/no/blog/tags/trevas"}],"readingTime":1.815,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-check_hierarchy","title":"Trevas - check_hierarchy","authors":["nicolas"],"tags":["Trevas"]},"prevItem":{"title":"Trevas - Persistent assignments","permalink":"/Trevas/no/blog/trevas-persistent-assignments"},"nextItem":{"title":"Trevas Batch 0.1.1","permalink":"/Trevas/no/blog/trevas-batch-0.1.1"}},"content":"### News\\n\\nTrevas 1.1.0 includes hierarchical validation via operators `define hierarchical ruleset` and `check_hierarchy`.\\n\\n### Example\\n\\n#### Input\\n\\n`ds1`:\\n\\n| id  | Me  |\\n| :-: | :-: |\\n| ABC | 12  |\\n|  A  |  1  |\\n|  B  | 10  |\\n|  C  |  1  |\\n| DEF | 100 |\\n|  E  | 99  |\\n|  F  |  1  |\\n| HIJ | 100 |\\n|  H  | 99  |\\n|  I  |  0  |\\n\\n#### VTL script\\n\\n```\\n// Ensure ds1 metadata definition is good\\nds1 := ds1[calc identifier id := id, Me := cast(Me, integer)];\\n\\n// Define hierarchical ruleset\\ndefine hierarchical ruleset hr (variable rule Me) is\\n    My_Rule : ABC = A + B + C errorcode \\"ABC is not sum of A,B,C\\" errorlevel 1;\\n    DEF = D + E + F errorcode \\"DEF is not sum of D,E,F\\";\\n    HIJ : HIJ = H + I - J errorcode \\"HIJ is not H + I - J\\" errorlevel 10\\nend hierarchical ruleset;\\n\\n// Check hierarchy\\nds_all := check_hierarchy(ds1, hr rule id all);\\nds_all_measures := check_hierarchy(ds1, hr rule id always_null all_measures);\\nds_invalid := check_hierarchy(ds1, hr rule id always_zero invalid);\\n```\\n\\n#### Outputs\\n\\n- `ds_all`\\n\\n| id  | ruleid  | bool_var | errorcode | errorlevel | imbalance |\\n| :-: | :-----: | :------: | :-------: | :--------: | :-------: |\\n| ABC | My_Rule |   true   |   null    |    null    |     0     |\\n\\n- `ds_always_null_all_measures`\\n\\n| id  | Me  | ruleid  | bool_var | errorcode | errorlevel | imbalance |\\n| :-: | :-: | :-----: | :------: | :-------: | :--------: | :-------: |\\n| ABC | 12  | My_Rule |   true   |   null    |    null    |     0     |\\n| DEF | 100 |  hr_2   |   null   |   null    |    null    |   null    |\\n| HIJ | 100 |   HIJ   |   null   |   null    |    null    |   null    |\\n\\n- `ds_invalid`\\n\\n| id  | Me  | ruleid |      errorcode       | errorlevel | imbalance |\\n| :-: | :-: | :----: | :------------------: | :--------: | :-------: |\\n| HIJ | 100 |  HIJ   | HIJ is not H + I - J |     10     |     1     |"},{"id":"/trevas-batch-0.1.1","metadata":{"permalink":"/Trevas/no/blog/trevas-batch-0.1.1","source":"@site/i18n/no/docusaurus-plugin-content-blog/2023-07-02-trevas-batch-0.1.1.mdx","title":"Trevas Batch 0.1.1","description":"Trevas Batch 0.1.1 uses version 1.0.2 of Trevas.","date":"2023-07-02T00:00:00.000Z","formattedDate":"2. juli 2023","tags":[{"label":"Trevas Batch","permalink":"/Trevas/no/blog/tags/trevas-batch"}],"readingTime":0.46,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-batch-0.1.1","title":"Trevas Batch 0.1.1","authors":["nicolas"],"tags":["Trevas Batch"]},"prevItem":{"title":"Trevas - check_hierarchy","permalink":"/Trevas/no/blog/trevas-check_hierarchy"},"nextItem":{"title":"Trevas Jupyter 0.3.2","permalink":"/Trevas/no/blog/trevas-jupyter-0.3.2"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Batch](https://github.com/Making-Sense-Info/Trevas-Batch) `0.1.1` uses version `1.0.2` of [Trevas](https://github.com/InseeFr/Trevas).\\n\\nThis Java batch provides Trevas execution metrics in Spark mode.\\n\\nThe configuration file to fill in is described in the [README](https://github.com/Making-Sense-Info/Trevas-Batch/tree/main#readme) of the project.\\nLaunching the batch will produce a Markdown file as output.\\n\\n### Launch\\n\\n#### Local\\n\\n```java\\njava -jar trevas-batch-0.1.1.jar -Dconfig.path=\\"...\\" -Dreport.path=\\"...\\"\\n```\\n\\nThe java execution will be done in local Spark.\\n\\n#### Kubernetes\\n\\nDefault Kubernetes objects are defined in the [.kubernetes](https://github.com/Making-Sense-Info/Trevas-Batch/tree/main/.kubernetes) folder.\\n\\nFeed the `config-map.yml` file then launch the job in your cluster."},{"id":"/trevas-jupyter-0.3.2","metadata":{"permalink":"/Trevas/no/blog/trevas-jupyter-0.3.2","source":"@site/i18n/no/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-jupyter-0.3.2.mdx","title":"Trevas Jupyter 0.3.2","description":"Trevas Jupyter 0.3.2 uses version 1.0.2 of Trevas.","date":"2023-07-01T00:00:00.000Z","formattedDate":"1. juli 2023","tags":[{"label":"Trevas Jupyter","permalink":"/Trevas/no/blog/tags/trevas-jupyter"}],"readingTime":0.59,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-jupyter-0.3.2","title":"Trevas Jupyter 0.3.2","authors":["nicolas"],"tags":["Trevas Jupyter"]},"prevItem":{"title":"Trevas Batch 0.1.1","permalink":"/Trevas/no/blog/trevas-batch-0.1.1"},"nextItem":{"title":"Trevas Lab 0.3.3","permalink":"/Trevas/no/blog/trevas-lab-0.3.3"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Jupyter](https://github.com/InseeFrLab/Trevas-Jupyter) `0.3.2` uses version `1.0.2` of [Trevas](https://github.com/InseeFr/Trevas).\\n\\n### News\\n\\nIn addition to the <Link label=\\"VTL coverage\\" href={useBaseUrl(\'/user-guide/coverage\')} /> greatly increased since the publication of Trevas 1.x.x, Trevas Jupyter offers 1 new connector:\\n\\n- SAS files (via the `loadSas` method)\\n\\n### Launch\\n\\n#### Manually adding the Trevas Kernel to an existing Jupyter instance\\n\\n- Trevas Jupyter compiler\\n- Copy the `kernel.json` file and the `bin` and `repo` folders to a new kernel folder.\\n- Edit the `kernel.json` file\\n- Launch Jupyter\\n\\n#### Docker\\n\\n```bash\\ndocker pull inseefrlab/trevas-jupyter:0.3.2\\ndocker run -p 8888:8888 inseefrlab/trevas-jupyter:0.3.2\\n```\\n\\n#### Helm\\n\\nThe Trevas Jupyter docker image can be instantiated via the `jupyter-pyspark` Helm contract from [InseeFrLab](https://github.com/InseeFrLab/helm-charts-interactive-services/tree/main)."},{"id":"/trevas-lab-0.3.3","metadata":{"permalink":"/Trevas/no/blog/trevas-lab-0.3.3","source":"@site/i18n/no/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-lab-0.3.3.mdx","title":"Trevas Lab 0.3.3","description":"Trevas Lab 0.3.3 uses version 1.0.2 of Trevas.","date":"2023-07-01T00:00:00.000Z","formattedDate":"1. juli 2023","tags":[{"label":"Trevas Lab","permalink":"/Trevas/no/blog/tags/trevas-lab"}],"readingTime":0.335,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-lab-0.3.3","title":"Trevas Lab 0.3.3","authors":["nicolas"],"tags":["Trevas Lab"]},"prevItem":{"title":"Trevas Jupyter 0.3.2","permalink":"/Trevas/no/blog/trevas-jupyter-0.3.2"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Lab](https://github.com/InseeFrLab/Trevas-Lab) `0.3.3` uses version `1.0.2` of [Trevas](https://github.com/InseeFr/Trevas).\\n\\n### News\\n\\nIn addition to the <Link label=\\"VTL coverage\\" href={useBaseUrl(\'/user-guide/coverage\')} /> greatly increased since the publication of Trevas 1.x.x, Trevas Lab offers 2 new connectors:\\n\\n- SAS files\\n- JDBC MariaDB\\n\\n### Launch\\n\\n#### Kubernetes\\n\\nSample Kubernetes objects are available in the `.kubernetes` folders of [Trevas Lab](https://github.com/InseeFrLab/Trevas-Lab/tree/master/.kubernetes) and [Trevas Lab UI](https://github.com/InseeFrLab/Trevas-Lab-UI/tree/master/.kubernetes)."}]}')}}]);