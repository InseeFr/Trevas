"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[9688],{8730:e=>{e.exports=JSON.parse('{"blogPosts":[{"id":"/trevas-1.1.0","metadata":{"permalink":"/Trevas/fr/blog/trevas-1.1.0","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-09-01-trevas-1.1.0.mdx","title":"Trevas 1.1.0","description":"Nouveaut\xe9s","date":"2023-09-01T00:00:00.000Z","formattedDate":"1 septembre 2023","tags":[{"label":"Trevas Jupyter","permalink":"/Trevas/fr/blog/tags/trevas-jupyter"}],"readingTime":1.855,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-1.1.0","title":"Trevas 1.1.0","authors":["nicolas"],"tags":["Trevas Jupyter"]},"nextItem":{"title":"Trevas Batch 0.1.1","permalink":"/Trevas/fr/blog/trevas-batch-0.1.1"}},"content":"### Nouveaut\xe9s\\n\\nLa validation hi\xe9rarchique est impl\xe9ment\xe9e dans Trevas 1.1.0, via les op\xe9rateurs `define hierarchical ruleset` et `check_hierarchy`.\\n\\n### Exemple\\n\\n#### Donn\xe9es en entr\xe9e\\n\\n`ds1`:\\n\\n| id  | Me  |\\n| :-: | :-: |\\n| ABC | 12  |\\n|  A  |  1  |\\n|  B  | 10  |\\n|  C  |  1  |\\n| DEF | 100 |\\n|  E  | 99  |\\n|  F  |  1  |\\n| HIJ | 100 |\\n|  H  | 99  |\\n|  I  |  0  |\\n\\n#### Script VTL\\n\\n```\\n// Ensure ds1 metadata definition is good\\nds1 := ds1[calc identifier id := id, Me := cast(Me, integer)];\\n\\n// Define hierarchical ruleset\\ndefine hierarchical ruleset hr (variable rule Me) is\\n    My_Rule : ABC = A + B + C errorcode \\"ABC is not sum of A,B,C\\" errorlevel 1;\\n    DEF = D + E + F errorcode \\"DEF is not sum of D,E,F\\";\\n    HIJ : HIJ = H + I - J errorcode \\"HIJ is not H + I - J\\" errorlevel 10\\nend hierarchical ruleset;\\n\\n// Check hierarchy\\nds_all := check_hierarchy(ds1, hr rule id all);\\nds_all_measures := check_hierarchy(ds1, hr rule id always_null all_measures);\\nds_invalid := check_hierarchy(ds1, hr rule id always_zero invalid);\\n```\\n\\n#### Donn\xe9es en sortie\\n\\n- `ds_all`\\n\\n| id  | ruleid  | bool_var | errorcode | errorlevel | imbalance |\\n| :-: | :-----: | :------: | :-------: | :--------: | :-------: |\\n| ABC | My_Rule |   true   |   null    |    null    |     0     |\\n\\n- `ds_always_null_all_measures`\\n\\n| id  | Me  | ruleid  | bool_var | errorcode | errorlevel | imbalance |\\n| :-: | :-: | :-----: | :------: | :-------: | :--------: | :-------: |\\n| ABC | 12  | My_Rule |   true   |   null    |    null    |     0     |\\n| DEF | 100 |  hr_2   |   null   |   null    |    null    |   null    |\\n| HIJ | 100 |   HIJ   |   null   |   null    |    null    |   null    |\\n\\n- `ds_invalid`\\n\\n| id  | Me  | ruleid |      errorcode       | errorlevel | imbalance |\\n| :-: | :-: | :----: | :------------------: | :--------: | :-------: |\\n| HIJ | 100 |  HIJ   | HIJ is not H + I - J |     10     |     1     |"},{"id":"/trevas-batch-0.1.1","metadata":{"permalink":"/Trevas/fr/blog/trevas-batch-0.1.1","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-07-02-trevas-batch-0.1.1.mdx","title":"Trevas Batch 0.1.1","description":"Trevas Batch 0.1.1 utilise la version 1.0.2 de Trevas.","date":"2023-07-02T00:00:00.000Z","formattedDate":"2 juillet 2023","tags":[{"label":"Trevas Batch","permalink":"/Trevas/fr/blog/tags/trevas-batch"}],"readingTime":0.47,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-batch-0.1.1","title":"Trevas Batch 0.1.1","authors":["nicolas"],"tags":["Trevas Batch"]},"prevItem":{"title":"Trevas 1.1.0","permalink":"/Trevas/fr/blog/trevas-1.1.0"},"nextItem":{"title":"Trevas Jupyter 0.3.2","permalink":"/Trevas/fr/blog/trevas-jupyter-0.3.2"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Batch](https://github.com/Making-Sense-Info/Trevas-Batch) `0.1.1` utilise la version `1.0.2` de [Trevas](https://github.com/InseeFr/Trevas).\\n\\nCe batch Java permet d\'obtenir des m\xe9triques d\'ex\xe9cutions Trevas en mode Spark.\\n\\nLe fichier de configuration \xe0 renseigner est d\xe9crit de le [README](https://github.com/Making-Sense-Info/Trevas-Batch/tree/main#readme) du projet.\\nLe lancement du batch produiera un fichier Markdown en sortie.\\n\\n### Lancement\\n\\n#### Local\\n\\n```java\\njava -jar trevas-batch-0.1.1.jar -Dconfig.path=\\"...\\" -Dreport.path=\\"...\\"\\n```\\n\\nL\'ex\xe9cution java se fera en Spark local.\\n\\n#### Kubernetes\\n\\nDes objets Kubernetes par d\xe9faut sont d\xe9finis dans le dossier [.kubernetes](https://github.com/Making-Sense-Info/Trevas-Batch/tree/main/.kubernetes).\\n\\nAlimenter le fichier `config-map.yml` puis lancer le job dans votre cluster."},{"id":"/trevas-jupyter-0.3.2","metadata":{"permalink":"/Trevas/fr/blog/trevas-jupyter-0.3.2","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-jupyter-0.3.2.mdx","title":"Trevas Jupyter 0.3.2","description":"Trevas Jupyter 0.3.2 utilise la version 1.0.2 de Trevas.","date":"2023-07-01T00:00:00.000Z","formattedDate":"1 juillet 2023","tags":[{"label":"Trevas Jupyter","permalink":"/Trevas/fr/blog/tags/trevas-jupyter"}],"readingTime":0.605,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-jupyter-0.3.2","title":"Trevas Jupyter 0.3.2","authors":["nicolas"],"tags":["Trevas Jupyter"]},"prevItem":{"title":"Trevas Batch 0.1.1","permalink":"/Trevas/fr/blog/trevas-batch-0.1.1"},"nextItem":{"title":"Trevas Lab 0.3.3","permalink":"/Trevas/fr/blog/trevas-lab-0.3.3"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Jupyter](https://github.com/InseeFrLab/Trevas-Jupyter) `0.3.2` utilise la version `1.0.2` de [Trevas](https://github.com/InseeFr/Trevas).\\n\\n### Nouveaut\xe9s\\n\\nEn suppl\xe9ment de la <Link label=\\"couverture VTL\\" href={useBaseUrl(\'/user-guide/coverage\')} /> largement augment\xe9e depuis la publication de Trevas 1.x.x, Trevas Jupyter propose 1 nouveau connecteur :\\n\\n- fichiers SAS (via la m\xe9thode `loadSas`)\\n\\n### Lancement\\n\\n#### Ajout manuel du Kernel Trevas \xe0 une instance Jupyter existante\\n\\n- Compiler Trevas Jupyter\\n- Copier le fichier `kernel.json` et les dossiers `bin` et `repo` dans un nouveau dossier de kernel.\\n- Editer le fichier `kernel.json`\\n- Lancer Jupyter\\n\\n#### Docker\\n\\n```bash\\ndocker pull inseefrlab/trevas-jupyter:0.3.2\\ndocker run -p 8888:8888 inseefrlab/trevas-jupyter:0.3.2\\n```\\n\\n#### Helm\\n\\nL\'image docker de Trevas Jupyter peut \xeatre instanci\xe9e via le contrat Helm `jupyter-pyspark` de [InseeFrLab](https://github.com/InseeFrLab/helm-charts-interactive-services/tree/main)."},{"id":"/trevas-lab-0.3.3","metadata":{"permalink":"/Trevas/fr/blog/trevas-lab-0.3.3","source":"@site/i18n/fr/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-lab-0.3.3.mdx","title":"Trevas Lab 0.3.3","description":"Trevas Lab 0.3.3 utilise la version 1.0.2 de Trevas.","date":"2023-07-01T00:00:00.000Z","formattedDate":"1 juillet 2023","tags":[{"label":"Trevas Lab","permalink":"/Trevas/fr/blog/tags/trevas-lab"}],"readingTime":0.35,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - D\xe9veloppeur","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-lab-0.3.3","title":"Trevas Lab 0.3.3","authors":["nicolas"],"tags":["Trevas Lab"]},"prevItem":{"title":"Trevas Jupyter 0.3.2","permalink":"/Trevas/fr/blog/trevas-jupyter-0.3.2"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Lab](https://github.com/InseeFrLab/Trevas-Lab) `0.3.3` utilise la version `1.0.2` de [Trevas](https://github.com/InseeFr/Trevas).\\n\\n### Nouveaut\xe9s\\n\\nEn suppl\xe9ment de la <Link label=\\"couverture VTL\\" href={useBaseUrl(\'/user-guide/coverage\')} /> largement augment\xe9e depuis la publication de Trevas 1.x.x, Trevas Lab propose 2 nouveaux connecteurs :\\n\\n- fichiers SAS\\n- JDBC MariaDB\\n\\n### Lancement\\n\\n#### Kubernetes\\n\\nDes exemples d\'objet Kubernetes sont disponibles dans les dossiers `.kubernetes` de [Trevas Lab](https://github.com/InseeFrLab/Trevas-Lab/tree/master/.kubernetes) et [Trevas Lab UI](https://github.com/InseeFrLab/Trevas-Lab-UI/tree/master/.kubernetes)."}]}')}}]);