# Coverage

Conformance tests for Trevas against the VTL Technology Compatibility Kit (TCK).

## TCK source

Tests load `src/main/resources/v2.1.zip`, built from the [SDMX-VTL](https://github.com/sdmx-twg/vtl) reference manual examples (`v2.1/docs/reference_manual/operators/**/examples`).

Until the TCK is published from an official VTL release branch, Trevas pins the **git branch** of `sdmx-twg/vtl` that contains corrected example fixtures (CSV/JSON):

| Context | Branch |
|---------|--------|
| CI (`tck-vtl-tf-spark3.yml`, `tck-vtl-tf-spark4.yml`) | `fix/doc-examples` (`VTL_TCK_BRANCH`) |
| Local (default in `refresh_tck_zip.sh`) | `fix/doc-examples` |

Upstream branch: https://github.com/sdmx-twg/vtl/tree/fix/doc-examples

The zip is **not** committed in this repository; CI and developers generate it before running `TCKTest`.

## Refresh the TCK zip locally

From the **Trevas repository root**:

```shell
./coverage/scripts/refresh_tck_zip.sh
```

This clones `sdmx-twg/vtl` at `fix/doc-examples`, runs `scripts/generate_tck_files.py` with `DOC_VERSION=v2.1`, and copies `tck/v2.1.zip` to `coverage/src/main/resources/v2.1.zip`.

Optional overrides:

```shell
VTL_TCK_BRANCH=fix/doc-examples ./coverage/scripts/refresh_tck_zip.sh
DOC_VERSION=v2.1 VTL_TCK_BRANCH=master ./coverage/scripts/refresh_tck_zip.sh
```

### Manual equivalent

```shell
git clone --depth 1 --branch fix/doc-examples https://github.com/sdmx-twg/vtl.git /tmp/vtl-tck
DOC_VERSION=v2.1 python3 /tmp/vtl-tck/scripts/generate_tck_files.py
mkdir -p coverage/src/main/resources
cp /tmp/vtl-tck/tck/v2.1.zip coverage/src/main/resources/v2.1.zip
```

If you already have a local `vtl` checkout on `fix/doc-examples`:

```shell
cd /path/to/vtl
git checkout fix/doc-examples
DOC_VERSION=v2.1 python3 scripts/generate_tck_files.py
cp tck/v2.1.zip /path/to/trevas/coverage/src/main/resources/v2.1.zip
```

## Run the TCK

After refreshing the zip:

```shell
mvn clean install -pl coverage -am -DskipTests --batch-mode
mvn test -pl coverage --batch-mode
```

Spark 4 variant:

```shell
mvn test -pl coverage -Pspark4-tck --batch-mode
```

Reports (after a test run):

```shell
python3 coverage/scripts/prettify_tck_surefire_xml.py
python3 coverage/scripts/render_tck_job_summary.py
# → coverage/target/tck-scripts-report.md (cases ordered by Test N, same as JUnit)
```

## CI

On each TCK workflow run, GitHub Actions:

1. Checks out Trevas
2. Clones `https://github.com/sdmx-twg/vtl.git` at `VTL_TCK_BRANCH` (`fix/doc-examples`)
3. Runs `DOC_VERSION=v2.1 python3 vtl/scripts/generate_tck_files.py`
4. Copies `vtl/tck/v2.1.zip` into `coverage/src/main/resources/`
5. Runs `mvn test -pl coverage`

No committed zip and no git hooks: local and CI use the same generation step.

## Related links

- [TCK automation PR (VTL TF)](https://github.com/sdmx-twg/vtl/pull/565)
- [Trevas TCK CSV loader notes](../roadmap/tck-csv-loader.md)
- JUnit / reporting: [junit5](https://github.com/junit-team/junit5/discussions/4504#discussioncomment-13046641), [surefire](https://github.com/apache/maven-surefire/issues/835), [dorny/test-reporter](https://github.com/dorny/test-reporter/issues/580)
