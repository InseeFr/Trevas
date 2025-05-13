# 📊 Coverage

## 🧪 TCK (Technology Compatibility Kit)

We are working on a suite of compatibility tests to ensure conformance with the VTL specification across implementations.

🛠️ _Work in Progress_

### Issues/discussions to follow

- [junit5](https://github.com/junit-team/junit5/discussions/4504#discussioncomment-13046641)
- [surefire](https://github.com/apache/maven-surefire/issues/835)
- [dorny/test-reporter](https://github.com/dorny/test-reporter/issues/580)

### Temporary run procedure

While [TCK](https://github.com/sdmx-twg/vtl/pull/565) is not automated in the VTL TF repository, we have to build the input source manually.

```shell
git clone https://github.com/sdmx-twg/vtl.git
cd vtl/scripts
DOC_VERSION=v2.1 python3 generate_tck_files.py
```

A zip will be created at `tck/v2.1.zip`.

Move it in Trevas resources:

```shell
mv vtl/tck/v2.1.zip trevas/coverage/src/main/resources
```

You are now able to run `TCKTest`.