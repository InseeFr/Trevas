"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[7763],{8232:e=>{e.exports=JSON.parse('{"blogPosts":[{"id":"/trevas-1.1.0","metadata":{"permalink":"/Trevas/zh-CN/blog/trevas-1.1.0","source":"@site/blog/2023-09-01-trevas-1.1.0.mdx","title":"Trevas 1.1.0","description":"News","date":"2023-09-01T00:00:00.000Z","formattedDate":"2023\u5e749\u67081\u65e5","tags":[{"label":"Trevas Jupyter","permalink":"/Trevas/zh-CN/blog/tags/trevas-jupyter"}],"readingTime":1.815,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-1.1.0","title":"Trevas 1.1.0","authors":["nicolas"],"tags":["Trevas Jupyter"]},"nextItem":{"title":"Trevas Batch 0.1.1","permalink":"/Trevas/zh-CN/blog/trevas-batch-0.1.1"}},"content":"### News\\n\\nTrevas 1.1.0 includes hierarchical validation via operators `define hierarchical ruleset` and `check_hierarchy`.\\n\\n### Example\\n\\n#### Input\\n\\n`ds1`:\\n\\n| id  | Me  |\\n| :-: | :-: |\\n| ABC | 12  |\\n|  A  |  1  |\\n|  B  | 10  |\\n|  C  |  1  |\\n| DEF | 100 |\\n|  E  | 99  |\\n|  F  |  1  |\\n| HIJ | 100 |\\n|  H  | 99  |\\n|  I  |  0  |\\n\\n#### VTL script\\n\\n```\\n// Ensure ds1 metadata definition is good\\nds1 := ds1[calc identifier id := id, Me := cast(Me, integer)];\\n\\n// Define hierarchical ruleset\\ndefine hierarchical ruleset hr (variable rule Me) is\\n    My_Rule : ABC = A + B + C errorcode \\"ABC is not sum of A,B,C\\" errorlevel 1;\\n    DEF = D + E + F errorcode \\"DEF is not sum of D,E,F\\";\\n    HIJ : HIJ = H + I - J errorcode \\"HIJ is not H + I - J\\" errorlevel 10\\nend hierarchical ruleset;\\n\\n// Check hierarchy\\nds_all := check_hierarchy(ds1, hr rule id all);\\nds_all_measures := check_hierarchy(ds1, hr rule id always_null all_measures);\\nds_invalid := check_hierarchy(ds1, hr rule id always_zero invalid);\\n```\\n\\n#### Outputs\\n\\n- `ds_all`\\n\\n| id  | ruleid  | bool_var | errorcode | errorlevel | imbalance |\\n| :-: | :-----: | :------: | :-------: | :--------: | :-------: |\\n| ABC | My_Rule |   true   |   null    |    null    |     0     |\\n\\n- `ds_always_null_all_measures`\\n\\n| id  | Me  | ruleid  | bool_var | errorcode | errorlevel | imbalance |\\n| :-: | :-: | :-----: | :------: | :-------: | :--------: | :-------: |\\n| ABC | 12  | My_Rule |   true   |   null    |    null    |     0     |\\n| DEF | 100 |  hr_2   |   null   |   null    |    null    |   null    |\\n| HIJ | 100 |   HIJ   |   null   |   null    |    null    |   null    |\\n\\n- `ds_invalid`\\n\\n| id  | Me  | ruleid |      errorcode       | errorlevel | imbalance |\\n| :-: | :-: | :----: | :------------------: | :--------: | :-------: |\\n| HIJ | 100 |  HIJ   | HIJ is not H + I - J |     10     |     1     |"},{"id":"/trevas-batch-0.1.1","metadata":{"permalink":"/Trevas/zh-CN/blog/trevas-batch-0.1.1","source":"@site/i18n/zh-CN/docusaurus-plugin-content-blog/2023-07-02-trevas-batch-0.1.1.mdx","title":"Trevas Batch 0.1.1","description":"Trevas Batch 0.1.1 uses version 1.0.2 of Trevas.","date":"2023-07-02T00:00:00.000Z","formattedDate":"2023\u5e747\u67082\u65e5","tags":[{"label":"Trevas Batch","permalink":"/Trevas/zh-CN/blog/tags/trevas-batch"}],"readingTime":0.46,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-batch-0.1.1","title":"Trevas Batch 0.1.1","authors":["nicolas"],"tags":["Trevas Batch"]},"prevItem":{"title":"Trevas 1.1.0","permalink":"/Trevas/zh-CN/blog/trevas-1.1.0"},"nextItem":{"title":"Trevas Jupyter 0.3.2","permalink":"/Trevas/zh-CN/blog/trevas-jupyter-0.3.2"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Batch](https://github.com/Making-Sense-Info/Trevas-Batch) `0.1.1` uses version `1.0.2` of [Trevas](https://github.com/InseeFr/Trevas).\\n\\nThis Java batch provides Trevas execution metrics in Spark mode.\\n\\nThe configuration file to fill in is described in the [README](https://github.com/Making-Sense-Info/Trevas-Batch/tree/main#readme) of the project.\\nLaunching the batch will produce a Markdown file as output.\\n\\n### Launch\\n\\n#### Local\\n\\n```java\\njava -jar trevas-batch-0.1.1.jar -Dconfig.path=\\"...\\" -Dreport.path=\\"...\\"\\n```\\n\\nThe java execution will be done in local Spark.\\n\\n#### Kubernetes\\n\\nDefault Kubernetes objects are defined in the [.kubernetes](https://github.com/Making-Sense-Info/Trevas-Batch/tree/main/.kubernetes) folder.\\n\\nFeed the `config-map.yml` file then launch the job in your cluster."},{"id":"/trevas-jupyter-0.3.2","metadata":{"permalink":"/Trevas/zh-CN/blog/trevas-jupyter-0.3.2","source":"@site/i18n/zh-CN/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-jupyter-0.3.2.mdx","title":"Trevas Jupyter 0.3.2","description":"Trevas Jupyter 0.3.2 uses version 1.0.2 of Trevas.","date":"2023-07-01T00:00:00.000Z","formattedDate":"2023\u5e747\u67081\u65e5","tags":[{"label":"Trevas Jupyter","permalink":"/Trevas/zh-CN/blog/tags/trevas-jupyter"}],"readingTime":0.59,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-jupyter-0.3.2","title":"Trevas Jupyter 0.3.2","authors":["nicolas"],"tags":["Trevas Jupyter"]},"prevItem":{"title":"Trevas Batch 0.1.1","permalink":"/Trevas/zh-CN/blog/trevas-batch-0.1.1"},"nextItem":{"title":"Trevas Lab 0.3.3","permalink":"/Trevas/zh-CN/blog/trevas-lab-0.3.3"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Jupyter](https://github.com/InseeFrLab/Trevas-Jupyter) `0.3.2` uses version `1.0.2` of [Trevas](https://github.com/InseeFr/Trevas).\\n\\n### News\\n\\nIn addition to the <Link label=\\"VTL coverage\\" href={useBaseUrl(\'/user-guide/coverage\')} /> greatly increased since the publication of Trevas 1.x.x, Trevas Jupyter offers 1 new connector:\\n\\n- SAS files (via the `loadSas` method)\\n\\n### Launch\\n\\n#### Manually adding the Trevas Kernel to an existing Jupyter instance\\n\\n- Trevas Jupyter compiler\\n- Copy the `kernel.json` file and the `bin` and `repo` folders to a new kernel folder.\\n- Edit the `kernel.json` file\\n- Launch Jupyter\\n\\n#### Docker\\n\\n```bash\\ndocker pull inseefrlab/trevas-jupyter:0.3.2\\ndocker run -p 8888:8888 inseefrlab/trevas-jupyter:0.3.2\\n```\\n\\n#### Helm\\n\\nThe Trevas Jupyter docker image can be instantiated via the `jupyter-pyspark` Helm contract from [InseeFrLab](https://github.com/InseeFrLab/helm-charts-interactive-services/tree/main)."},{"id":"/trevas-lab-0.3.3","metadata":{"permalink":"/Trevas/zh-CN/blog/trevas-lab-0.3.3","source":"@site/i18n/zh-CN/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-lab-0.3.3.mdx","title":"Trevas Lab 0.3.3","description":"Trevas Lab 0.3.3 uses version 1.0.2 of Trevas.","date":"2023-07-01T00:00:00.000Z","formattedDate":"2023\u5e747\u67081\u65e5","tags":[{"label":"Trevas Lab","permalink":"/Trevas/zh-CN/blog/tags/trevas-lab"}],"readingTime":0.335,"hasTruncateMarker":false,"authors":[{"name":"Nicolas Laval","link":"https://github.com/NicoLaval","title":"Making Sense - Developer","image":"profile_pic_nicolas_laval.jpg","key":"nicolas"}],"frontMatter":{"slug":"/trevas-lab-0.3.3","title":"Trevas Lab 0.3.3","authors":["nicolas"],"tags":["Trevas Lab"]},"prevItem":{"title":"Trevas Jupyter 0.3.2","permalink":"/Trevas/zh-CN/blog/trevas-jupyter-0.3.2"}},"content":"import useBaseUrl from \'@docusaurus/useBaseUrl\';\\nimport Link from \'@theme/Link\';\\n\\n[Trevas Lab](https://github.com/InseeFrLab/Trevas-Lab) `0.3.3` uses version `1.0.2` of [Trevas](https://github.com/InseeFr/Trevas).\\n\\n### News\\n\\nIn addition to the <Link label=\\"VTL coverage\\" href={useBaseUrl(\'/user-guide/coverage\')} /> greatly increased since the publication of Trevas 1.x.x, Trevas Lab offers 2 new connectors:\\n\\n- SAS files\\n- JDBC MariaDB\\n\\n### Launch\\n\\n#### Kubernetes\\n\\nSample Kubernetes objects are available in the `.kubernetes` folders of [Trevas Lab](https://github.com/InseeFrLab/Trevas-Lab/tree/master/.kubernetes) and [Trevas Lab UI](https://github.com/InseeFrLab/Trevas-Lab-UI/tree/master/.kubernetes)."}]}')}}]);