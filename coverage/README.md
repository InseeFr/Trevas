# Coverage — TCK VTL local

Module Maven qui exécute le **Technology Compatibility Kit (TCK)** VTL v2.1 contre Trevas (moteur + backend Spark).

**Point d’entrée JUnit :** `fr.insee.vtl.coverage.TCKTest`  
**Fichier requis :** `coverage/src/main/resources/v2.1.zip` (non versionné — à générer une fois)

---

## Démarrage rapide (depuis la racine du dépôt Trevas)

Prérequis : **JDK 17+**, **Maven 3.9+**, **Python 3**, accès réseau (clone GitHub la première fois).

```shell
# 1) Générer le zip TCK (une fois, ou après mise à jour des exemples SDMX-VTL)
./coverage/scripts/refresh_tck_zip.sh

# 2) Compiler les modules dont dépend coverage (dont vtl-csv, vtl-spark, vtl-engine…)
mvn clean install -pl coverage -am -DskipTests --batch-mode

# 3) Lancer le TCK (Spark 3 par défaut)
mvn test -pl coverage --batch-mode
```

Vérifier que le zip est bien là avant l’étape 3 :

```shell
test -f coverage/src/main/resources/v2.1.zip && echo OK || echo "manquant — relancer refresh_tck_zip.sh"
```

### Spark 4

```shell
mvn clean install -pl coverage -am -DskipTests --batch-mode -Pspark4-tck
mvn test -pl coverage --batch-mode -Pspark4-tck
```

### Rapport lisible après les tests

```shell
python3 coverage/scripts/prettify_tck_surefire_xml.py
python3 coverage/scripts/render_tck_job_summary.py
# → coverage/target/tck-scripts-report.md
```

XML Surefire principal : `coverage/target/surefire-reports/TEST-fr.insee.vtl.coverage.TCKTest.xml`

---

## Pourquoi `mvn test -pl coverage` seul échoue souvent

| Symptôme | Cause | Correctif |
|----------|--------|-----------|
| `FileNotFoundException` / zip introuvable | Pas de `v2.1.zip` | `./coverage/scripts/refresh_tck_zip.sh` |
| `ClassNotFoundException: fr.insee.vtl.csv.CsvDatasetValidator` | Module `vtl-csv` non installé | `mvn install -pl vtl-csv -am -DskipTests` ou **toujours** utiliser `-am` avec `coverage` |
| Tests TCK absents / 0 test | Classpath incomplet | `mvn clean install -pl coverage -am -DskipTests` puis `mvn test -pl coverage` |

**Règle :** pour `coverage`, utiliser **`-pl coverage -am`** (`-am` = *also make* : construit tous les modules requis du réacteur, dont `vtl-csv`).

---

## D’où vient le TCK ?

Les cas sont extraits des exemples du manuel VTL SDMX  
(`v2.1/docs/reference_manual/operators/**/examples`).

Branche Git pinée (fixtures CSV/JSON corrigées) : **`fix/doc-examples`**  
https://github.com/sdmx-twg/vtl/tree/fix/doc-examples

Le script local reproduit ce que fait la CI :

```shell
./coverage/scripts/refresh_tck_zip.sh
```

→ clone `sdmx-twg/vtl`, exécute `scripts/generate_tck_files.py`, copie `tck/v2.1.zip` vers `coverage/src/main/resources/v2.1.zip`.

Variables optionnelles :

```shell
VTL_TCK_BRANCH=fix/doc-examples ./coverage/scripts/refresh_tck_zip.sh
DOC_VERSION=v2.1 VTL_TCK_BRANCH=master ./coverage/scripts/refresh_tck_zip.sh
```

### Génération manuelle (équivalent)

```shell
git clone --depth 1 --branch fix/doc-examples https://github.com/sdmx-twg/vtl.git /tmp/vtl-tck
DOC_VERSION=v2.1 python3 /tmp/vtl-tck/scripts/generate_tck_files.py
mkdir -p coverage/src/main/resources
cp /tmp/vtl-tck/tck/v2.1.zip coverage/src/main/resources/v2.1.zip
```

---

## Lancer un sous-ensemble (debug)

```shell
# un seul cas (adapter le nom affiché dans les logs / XML)
mvn test -pl coverage -Dtest='TCKTest#leafCases' --batch-mode

# tests unitaires du module coverage (hors TCK long)
mvn test -pl coverage -Dtest='!TCKTest' --batch-mode
```

---

## CI

Workflows : `.github/workflows/tck-vtl-tf-spark3.yml`, `tck-vtl-tf-spark4.yml`

En résumé : checkout Trevas → génération `v2.1.zip` → `mvn clean install -pl coverage -am -DskipTests` → `mvn test -pl coverage` → rapport Surefire / résumé Markdown.

---

## Liens

- [TCK automation PR (VTL TF)](https://github.com/sdmx-twg/vtl/pull/565)
- [Notes chargeur CSV TCK](../roadmap/tck-csv-loader.md)
- JUnit / reporting : [junit5](https://github.com/junit-team/junit5/discussions/4504#discussioncomment-13046641), [surefire](https://github.com/apache/maven-surefire/issues/835), [dorny/test-reporter](https://github.com/dorny/test-reporter/issues/580)
