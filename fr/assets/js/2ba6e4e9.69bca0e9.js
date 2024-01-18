"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[9688],{98730:e=>{e.exports=JSON.parse('{"blogPosts":[{"id":"/trevas-java-17","metadata":{"permalink":"/Trevas/fr/blog/trevas-java-17","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-11-22-trevas-java-17.mdx","title":"Trevas - Java 17","description":"Nouveaut\xe9s","date":"2023-11-22T00:00:00.000Z","formattedDate":"22 novembre 2023","tags":[{"label":"Trevas","permalink":"/Trevas/fr/blog/tags/trevas"}],"readingTime":0.365,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-java-17","title":"Trevas - Java 17","authors":["nicolas"],"tags":["Trevas"]},"nextItem":{"title":"Trevas - Assignements persistants","permalink":"/Trevas/fr/blog/trevas-persistent-assignment"}},"content":"### Nouveaut\xe9s\\n\\nTrevas 1.2.0 permet le support de Java 17.\\n\\n### Gestion des modules Java\\n\\nSpark ne supporte pas les modules Java.\\n\\nLes applications clientes en Java 17 embarquant Trevas doivent configurer les `UNNAMED` modules pour Spark.\\n\\n#### Maven\\n\\nAjouter \xe0 votre fichier `pom.xml`, dans la section `build > plugins` :\\n\\n```xml\\n<plugin>\\n    <groupId>org.apache.maven.plugins</groupId>\\n    <artifactId>maven-compiler-plugin</artifactId>\\n    <version>3.11.0</version>\\n    <configuration>\\n        <compilerArgs>\\n            <arg>--add-exports</arg>\\n            <arg>java.base/sun.nio.ch=ALL-UNNAMED</arg>\\n        </compilerArgs>\\n    </configuration>\\n</plugin>\\n```\\n\\n#### Docker\\n\\n```shell\\nENTRYPOINT [\\"java\\", \\"--add-exports\\", \\"java.base/sun.nio.ch=ALL-UNNAMED\\", \\"mainClass\\"]\\n```"},{"id":"/trevas-persistent-assignment","metadata":{"permalink":"/Trevas/fr/blog/trevas-persistent-assignment","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-11-22-trevas-persistent-assignment.mdx","title":"Trevas - Assignements persistants","description":"Nouveaut\xe9s","date":"2023-11-22T00:00:00.000Z","formattedDate":"22 novembre 2023","tags":[{"label":"Trevas","permalink":"/Trevas/fr/blog/tags/trevas"}],"readingTime":0.44,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-persistent-assignment","title":"Trevas - Assignements persistants","authors":["nicolas"],"tags":["Trevas"]},"prevItem":{"title":"Trevas - Java 17","permalink":"/Trevas/fr/blog/trevas-java-17"},"nextItem":{"title":"Trevas - check_hierarchy","permalink":"/Trevas/fr/blog/trevas-check_hierarchy"}},"content":"### Nouveaut\xe9s\\n\\nTrevas 1.2.0 inclut le support des assignements persistants : `ds1 <- ds;`.\\n\\nDans Trevas, les datatsets persistants sont repr\xe9sent\xe9s par `PersistentDataset`.\\n\\n### G\xe9rer `PersistentDataset`\\n\\nLes datasets Trevas sont repr\xe9sent\xe9s par `Dataset`.\\n\\nApr\xe8s avoir execut\xe9 le moteur Trevas, vous pouvez utiliser les datasets persistants comme suit :\\n\\n```\\nBindings engineBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);\\nengineBindings.forEach((k, v) -> {\\n    if (v instanceof PersistentDataset) {\\n        fr.insee.vtl.model.Dataset ds = ((PersistentDataset) v).getDelegate();\\n        if (ds instanceof SparkDataset) {\\n            Dataset<Row> sparkDs = ((SparkDataset) ds).getSparkDataset();\\n            // Do what you want with sparkDs\\n        }\\n    }\\n});\\n```"},{"id":"/trevas-check_hierarchy","metadata":{"permalink":"/Trevas/fr/blog/trevas-check_hierarchy","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-09-01-trevas-check_hierarchy.mdx","title":"Trevas - check_hierarchy","description":"Nouveaut\xe9s","date":"2023-09-01T00:00:00.000Z","formattedDate":"1 septembre 2023","tags":[{"label":"Trevas","permalink":"/Trevas/fr/blog/tags/trevas"}],"readingTime":1.855,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-check_hierarchy","title":"Trevas - check_hierarchy","authors":["nicolas"],"tags":["Trevas"]},"prevItem":{"title":"Trevas - Assignements persistants","permalink":"/Trevas/fr/blog/trevas-persistent-assignment"},"nextItem":{"title":"Trevas Batch 0.1.1","permalink":"/Trevas/fr/blog/trevas-batch-0.1.1"}},"content":"### Nouveaut\xe9s\\n\\nLa validation hi\xe9rarchique est impl\xe9ment\xe9e dans Trevas 1.1.0, via les op\xe9rateurs `define hierarchical ruleset` et `check_hierarchy`.\\n\\n### Exemple\\n\\n#### Donn\xe9es en entr\xe9e\\n\\n`ds1`:\\n\\n| id  | Me  |\\n| :-: | :-: |\\n| ABC | 12  |\\n|  A  |  1  |\\n|  B  | 10  |\\n|  C  |  1  |\\n| DEF | 100 |\\n|  E  | 99  |\\n|  F  |  1  |\\n| HIJ | 100 |\\n|  H  | 99  |\\n|  I  |  0  |\\n\\n#### Script VTL\\n\\n```\\n// Ensure ds1 metadata definition is good\\nds1 := ds1[calc identifier id := id, Me := cast(Me, integer)];\\n\\n// Define hierarchical ruleset\\ndefine hierarchical ruleset hr (variable rule Me) is\\n    My_Rule : ABC = A + B + C errorcode \\"ABC is not sum of A,B,C\\" errorlevel 1;\\n    DEF = D + E + F errorcode \\"DEF is not sum of D,E,F\\";\\n    HIJ : HIJ = H + I - J errorcode \\"HIJ is not H + I - J\\" errorlevel 10\\nend hierarchical ruleset;\\n\\n// Check hierarchy\\nds_all := check_hierarchy(ds1, hr rule id all);\\nds_all_measures := check_hierarchy(ds1, hr rule id always_null all_measures);\\nds_invalid := check_hierarchy(ds1, hr rule id always_zero invalid);\\n```\\n\\n#### Donn\xe9es en sortie\\n\\n- `ds_all`\\n\\n| id  | ruleid  | bool_var | errorcode | errorlevel | imbalance |\\n| :-: | :-----: | :------: | :-------: | :--------: | :-------: |\\n| ABC | My_Rule |   true   |   null    |    null    |     0     |\\n\\n- `ds_always_null_all_measures`\\n\\n| id  | Me  | ruleid  | bool_var | errorcode | errorlevel | imbalance |\\n| :-: | :-: | :-----: | :------: | :-------: | :--------: | :-------: |\\n| ABC | 12  | My_Rule |   true   |   null    |    null    |     0     |\\n| DEF | 100 |  hr_2   |   null   |   null    |    null    |   null    |\\n| HIJ | 100 |   HIJ   |   null   |   null    |    null    |   null    |\\n\\n- `ds_invalid`\\n\\n| id  | Me  | ruleid |      errorcode       | errorlevel | imbalance |\\n| :-: | :-: | :----: | :------------------: | :--------: | :-------: |\\n| HIJ | 100 |  HIJ   | HIJ is not H + I - J |     10     |     1     |"},{"id":"/trevas-batch-0.1.1","metadata":{"permalink":"/Trevas/fr/blog/trevas-batch-0.1.1","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-07-02-trevas-batch-0.1.1.mdx","title":"Trevas Batch 0.1.1","description":"Trevas Batch 0.1.1 utilise la version 1.0.2 de Trevas.","date":"2023-07-02T00:00:00.000Z","formattedDate":"2 juillet 2023","tags":[{"label":"Trevas Batch","permalink":"/Trevas/fr/blog/tags/trevas-batch"}],"readingTime":0.47,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-batch-0.1.1","title":"Trevas Batch 0.1.1","authors":["nicolas"],"tags":["Trevas Batch"]},"prevItem":{"title":"Trevas - check_hierarchy","permalink":"/Trevas/fr/blog/trevas-check_hierarchy"},"nextItem":{"title":"Trevas Jupyter 0.3.2","permalink":"/Trevas/fr/blog/trevas-jupyter-0.3.2"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Batch](https://github.com/Making-Sense-Info/Trevas-Batch) `0.1.1` utilise la version `1.0.2` de [Trevas](https://github.com/InseeFr/Trevas).\\n\\nCe batch Java permet d\'obtenir des m\xe9triques d\'ex\xe9cutions Trevas en mode Spark.\\n\\nLe fichier de configuration \xe0 renseigner est d\xe9crit de le [README](https://github.com/Making-Sense-Info/Trevas-Batch/tree/main#readme) du projet.\\nLe lancement du batch produiera un fichier Markdown en sortie.\\n\\n### Lancement\\n\\n#### Local\\n\\n```java\\njava -jar trevas-batch-0.1.1.jar -Dconfig.path=\\"...\\" -Dreport.path=\\"...\\"\\n```\\n\\nL\'ex\xe9cution java se fera en Spark local.\\n\\n#### Kubernetes\\n\\nDes objets Kubernetes par d\xe9faut sont d\xe9finis dans le dossier [.kubernetes](https://github.com/Making-Sense-Info/Trevas-Batch/tree/main/.kubernetes).\\n\\nAlimenter le fichier `config-map.yml` puis lancer le job dans votre cluster."},{"id":"/trevas-jupyter-0.3.2","metadata":{"permalink":"/Trevas/fr/blog/trevas-jupyter-0.3.2","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-jupyter-0.3.2.mdx","title":"Trevas Jupyter 0.3.2","description":"Trevas Jupyter 0.3.2 utilise la version 1.0.2 de Trevas.","date":"2023-07-01T00:00:00.000Z","formattedDate":"1 juillet 2023","tags":[{"label":"Trevas Jupyter","permalink":"/Trevas/fr/blog/tags/trevas-jupyter"}],"readingTime":0.605,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-jupyter-0.3.2","title":"Trevas Jupyter 0.3.2","authors":["nicolas"],"tags":["Trevas Jupyter"]},"prevItem":{"title":"Trevas Batch 0.1.1","permalink":"/Trevas/fr/blog/trevas-batch-0.1.1"},"nextItem":{"title":"Trevas Lab 0.3.3","permalink":"/Trevas/fr/blog/trevas-lab-0.3.3"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Jupyter](https://github.com/InseeFrLab/Trevas-Jupyter) `0.3.2` utilise la version `1.0.2` de [Trevas](https://github.com/InseeFr/Trevas).\\n\\n### Nouveaut\xe9s\\n\\nEn suppl\xe9ment de la <Link label=\\"couverture VTL\\" href={useBaseUrl(\'/user-guide/coverage\')} /> largement augment\xe9e depuis la publication de Trevas 1.x.x, Trevas Jupyter propose 1 nouveau connecteur :\\n\\n- fichiers SAS (via la m\xe9thode `loadSas`)\\n\\n### Lancement\\n\\n#### Ajout manuel du Kernel Trevas \xe0 une instance Jupyter existante\\n\\n- Compiler Trevas Jupyter\\n- Copier le fichier `kernel.json` et les dossiers `bin` et `repo` dans un nouveau dossier de kernel.\\n- Editer le fichier `kernel.json`\\n- Lancer Jupyter\\n\\n#### Docker\\n\\n```bash\\ndocker pull inseefrlab/trevas-jupyter:0.3.2\\ndocker run -p 8888:8888 inseefrlab/trevas-jupyter:0.3.2\\n```\\n\\n#### Helm\\n\\nL\'image docker de Trevas Jupyter peut \xeatre instanci\xe9e via le contrat Helm `jupyter-pyspark` de [InseeFrLab](https://github.com/InseeFrLab/helm-charts-interactive-services/tree/main)."},{"id":"/trevas-lab-0.3.3","metadata":{"permalink":"/Trevas/fr/blog/trevas-lab-0.3.3","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-lab-0.3.3.mdx","title":"Trevas Lab 0.3.3","description":"Trevas Lab 0.3.3 utilise la version 1.0.2 de Trevas.","date":"2023-07-01T00:00:00.000Z","formattedDate":"1 juillet 2023","tags":[{"label":"Trevas Lab","permalink":"/Trevas/fr/blog/tags/trevas-lab"}],"readingTime":0.35,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-lab-0.3.3","title":"Trevas Lab 0.3.3","authors":["nicolas"],"tags":["Trevas Lab"]},"prevItem":{"title":"Trevas Jupyter 0.3.2","permalink":"/Trevas/fr/blog/trevas-jupyter-0.3.2"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Lab](https://github.com/InseeFrLab/Trevas-Lab) `0.3.3` utilise la version `1.0.2` de [Trevas](https://github.com/InseeFr/Trevas).\\n\\n### Nouveaut\xe9s\\n\\nEn suppl\xe9ment de la <Link label=\\"couverture VTL\\" href={useBaseUrl(\'/user-guide/coverage\')} /> largement augment\xe9e depuis la publication de Trevas 1.x.x, Trevas Lab propose 2 nouveaux connecteurs :\\n\\n- fichiers SAS\\n- JDBC MariaDB\\n\\n### Lancement\\n\\n#### Kubernetes\\n\\nDes exemples d\'objet Kubernetes sont disponibles dans les dossiers `.kubernetes` de [Trevas Lab](https://github.com/InseeFrLab/Trevas-Lab/tree/master/.kubernetes) et [Trevas Lab UI](https://github.com/InseeFrLab/Trevas-Lab-UI/tree/master/.kubernetes)."}]}')}}]);