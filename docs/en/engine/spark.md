# Dataset - Spark

The `SparkDataset` represent statistical cubes in Java, and delegate computations to a Spark cluster.

## Usage

- add required Trevas / Spark dependencies
- instanciate a Spark session
- instanciate a Trevas / Spark engine
- create the Spark data sources and feed them to the Trevas engine as `SparkDataset` instances

## Dependencies

```xml=
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-engine</artifactId>
    <version>0.1.0</version>
</dependency>
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-spark</artifactId>
    <version>0.1.0</version>
</dependency>
<!-- Only for Kubernetes case -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-kubernetes_2.12</artifactId>
    <version>3.0.1</version>
</dependency>
```

## Spark session

### Local

```java=
SparkSession.Builder sparkBuilder = SparkSession.builder()
        .appName("my-app")
        .master("local");

SparkSession spark = sparkBuilder.getOrCreate();
```

### Standalone

#### Install a standalone Spark cluster

The Spark cluster can be installed in any environment.

For example, installing a Spark cluster in a Kubernetes cluster via Helm is done with:

```bash
helm install my-spark bitnami/spark --set image.repository=inseefrlab/spark --set image.tag=latest
```

#### Instanciate the Spark session

```java=
SparkSession.Builder sparkBuilder = SparkSession.builder()
        .appName("vtl-lab")
        // my-spark is the name previously given to the cluster
        .master("spark://my-spark-master-svc:7077");

// Activate dynamic allocation of executors (optional)
sparkBuilder.config("spark.dynamicAllocation.enabled", true);
sparkBuilder.config("spark.dynamicAllocation.shuffleTracking.enabled", true);
// Minimum number of executors
sparkBuilder.config("spark.dynamicAllocation.minExecutors", 10);

// Connection to a S3 HDFS datalake
sparkBuilder.config("spark.hadoop.fs.s3a.access.key", sparkProperties.getAccessKey());
sparkBuilder.config("spark.hadoop.fs.s3a.secret.key", sparkProperties.getSecretKey());
sparkBuilder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", sparkProperties.getSslEnabled());
sparkBuilder.config("spark.hadoop.fs.s3a.session.token", sparkProperties.getSessionToken());
sparkBuilder.config("spark.hadoop.fs.s3a.endpoint", sparkProperties.getSessionEndpoint());

// Transfer Spark dependencies to the Spark executors
// See https://stackoverflow.com/questions/28079307
sparkBuilder.config("spark.jars", String.join(",",
        "/vtl-spark.jar",
        "/vtl-model.jar",
        "/vtl-jackson.jar",
        "/vtl-parser.jar",
        "/vtl-engine.jar"
));

SparkSession spark = sparkBuilder.getOrCreate();
```

### Kubernetes

Instead of connecting to a previously installed Spark instance (standalone mode), Kubernetes allows the orchestration of the execution of a Spark job in the cluster.

### Service account

A service account must be configured to allow the client application that queries the Kubernetes cluster to obtain the rights required to launch the job:

- `serviceaccount.yml`

Declare the service account associated to a Kubernetes cluster:

```yml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: my-service-account
  namespace: my-namespace
```

- `role.yml`

Declare the roles associated to a Kubernetes namespace:

```yml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: my-role
  namespace: my-namespace
rules:
  - apiGroups:
      - ""
    resources:
      - "pods"
      - "services"
      - "configmaps"
    verbs:
      - "list"
      - "create"
      - "delete"
      - "watch"
      - "update"
      - "get"
      - "patch"
```

- `role-bindings.yml`

Associate the service account and the role in the Kubernetes namespace:

```yml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: my-role-bindings
subjects:
  - kind: ServiceAccount
    name: my-service-account
    namespace: my-namespace
roleRef:
  kind: Role
  name: my-role
  apiGroup: rbac.authorization.k8s.io
```

- `deployment.yml`

Define the service account to use in the client Java application:

```yml
[...]
spec:
  template:
    spec:
      serviceAccountName: vtl-lab-sa
[...]
```

#### Configuration of the Spark Kubernetes session

```java=
SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("my-app")
                .master("k8s://https://kubernetes.default.svc.cluster.local:443");

// Docker image deployed for the driver and the executors
sparkBuilder.config("spark.kubernetes.container.image.pullPolicy", sparkProperties.getKubernetesContainerImagePullPolicy());
sparkBuilder.config("spark.kubernetes.container.image", sparkProperties.getKubernetesContainerImage());

// Configuration of the Spark instance to deploy in the Kubernetes cluster
sparkBuilder.config("spark.kubernetes.namespace", sparkProperties.getKubernetesNamespace());
sparkBuilder.config("spark.kubernetes.executor.request.cores", sparkProperties.getKubernetesExecutorRequestCores());
sparkBuilder.config("spark.kubernetes.driver.pod.name", sparkProperties.getKubernetesDriverPodName());

// Activate the dynamic allocation of executors (optional)
sparkBuilder.config("spark.dynamicAllocation.enabled", true);
sparkBuilder.config("spark.dynamicAllocation.shuffleTracking.enabled", true);
// Minimum number of executors
sparkBuilder.config("spark.dynamicAllocation.minExecutors", 10);

// Connection to a type 3 HDFS datalake
sparkBuilder.config("spark.hadoop.fs.s3a.access.key", sparkProperties.getAccessKey());
sparkBuilder.config("spark.hadoop.fs.s3a.secret.key", sparkProperties.getSecretKey());
sparkBuilder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", sparkProperties.getSslEnabled());
sparkBuilder.config("spark.hadoop.fs.s3a.session.token", sparkProperties.getSessionToken());
sparkBuilder.config("spark.hadoop.fs.s3a.endpoint", sparkProperties.getSessionEndpoint());

// Transfer the Trevas dependencies to the Spark executors
// See https://stackoverflow.com/questions/28079307
sparkBuilder.config("spark.jars", String.join(",",
        "/vtl-spark.jar",
        "/vtl-model.jar",
        "/vtl-jackson.jar",
        "/vtl-parser.jar",
        "/vtl-engine.jar"
));

SparkSession spark = sparkBuilder.getOrCreate();
```

## Trevas / Spark execution engine

By defaults, a Trevas engine resolves treatments `InMemory`.

In order to activate the resolution via Spark, two properties must be valued:

```java=
ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
ScriptContext context = engine.getContext();
context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
engine.put("$vtl.engine.processing_engine_names", "spark");
engine.put("$vtl.spark.session", spark);
```

## SparkDataset

In order to instanciate a Trevas `SparkDataset`, a Spark data source and a JSON file describing the data structure are required.

### Spark data source

The Spark API allows the creation of `Dataset` instances from various formats : Parquet, Avro, JSON, CSV, JDBC...

#### Parquet

```java=
Dataset<Row> dataset = spark.read().parquet("path/to/parquet/folder");
```

### Data structure

#### Format

```json
[
  { "name": "id", "type": "STRING", "role": "IDENTIFIER" },
  { "name": "x", "type": "INTEGER", "role": "MEASURE" },
  { "name": "y", "type": "FLOAT", "role": "MEASURE" }
]
```

#### Serialization

```java=
List<Structured.Component> components =
objectMapper.readValue(Paths.get("path/to/structure.json")
                .toFile(),
        new TypeReference<>() {
        });
Structured.DataStructure structure = new Structured.DataStructure(components);
```

### `SparkDataset` instanciation

```java=
SparkDataset dataset = new SparkDataset(dataset, structure);
```
