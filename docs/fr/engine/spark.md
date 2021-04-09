# Dataset - Spark

Les datasets `SparkDataset` permettent de représenter les tables statistiques dans une application Java, délégant les calculs à un cluster Spark.

## Utilisation

- ajouter les dépendances Trevas / Spark requises
- instancier une session Spark
- instancier un moteur d'éxecution Trevas / Spark
- créer des sources de données Spark et les introduire dans le moteur Trevas sous forme de `SparkDataset`

## Dépendances

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

## Session Spark

### Local

```java=
SparkSession.Builder sparkBuilder = SparkSession.builder()
        .appName("my-app")
        .master("local");

SparkSession spark = sparkBuilder.getOrCreate();
```

### Standalone

#### Installer un cluster Spark standalone

Le cluster Spark peut être installé dans n'importe quel environnement.

Par exemple, pour en installer un dans un cluster Kubernetes via Helm :

```bash
helm install my-spark bitnami/spark --set image.repository=inseefrlab/spark --set image.tag=latest
```

#### Instancier la session Spark

```java=
SparkSession.Builder sparkBuilder = SparkSession.builder()
        .appName("vtl-lab")
        // my-spark est le nom donné précédemment à votre cluster
        .master("spark://my-spark-master-svc:7077");

// Activer l'allocation dynamique d'exécuteur (optionnel)
sparkBuilder.config("spark.dynamicAllocation.enabled", true);
sparkBuilder.config("spark.dynamicAllocation.shuffleTracking.enabled", true);
// Nombre d'exécuteurs minimum
sparkBuilder.config("spark.dynamicAllocation.minExecutors", 10);

// Connection à un datalake HDFS de type s3
sparkBuilder.config("spark.hadoop.fs.s3a.access.key", sparkProperties.getAccessKey());
sparkBuilder.config("spark.hadoop.fs.s3a.secret.key", sparkProperties.getSecretKey());
sparkBuilder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", sparkProperties.getSslEnabled());
sparkBuilder.config("spark.hadoop.fs.s3a.session.token", sparkProperties.getSessionToken());
sparkBuilder.config("spark.hadoop.fs.s3a.endpoint", sparkProperties.getSessionEndpoint());

// Transférer les dépendances Trevas aux exécuteurs Spark
// Voir https://stackoverflow.com/questions/28079307
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

Plutôt que de se connecter à une instance Spark préalablement installée (mode standalone), Kubernetes permet d'orchestrer l'exécution d'un job Spark au sein du cluster.

### Service account

Afin que l'application cliente, requêtant le cluster Kubernetes pour lancer un job Spark, puisse obtenir les droits pour déployer les services nécessaires au lancement du job, un service account doit être configuré :

- `serviceaccount.yml`

Déclarer le service account associé à un namespace Kubernetes

```yml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: my-service-account
  namespace: my-namespace
```

- `role.yml`

Déclarer les rôles associés à un namespace Kubernetes

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

Associer le service account et le rôle dans le namespace Kubernetes

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

Définir le service account à utiliser dans l'application Java cliente

```yml
[...]
spec:
  template:
    spec:
      serviceAccountName: vtl-lab-sa
[...]
```

#### Configuration de la session Spark Kubernetes

```java=
SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("my-app")
                .master("k8s://https://kubernetes.default.svc.cluster.local:443");

// Image docker déployée pour le driver et les exécuteurs
sparkBuilder.config("spark.kubernetes.container.image.pullPolicy", sparkProperties.getKubernetesContainerImagePullPolicy());
sparkBuilder.config("spark.kubernetes.container.image", sparkProperties.getKubernetesContainerImage());

// Configuration de l'instance Spark à déployer dans le cluster Kubernetes
sparkBuilder.config("spark.kubernetes.namespace", sparkProperties.getKubernetesNamespace());
sparkBuilder.config("spark.kubernetes.executor.request.cores", sparkProperties.getKubernetesExecutorRequestCores());
sparkBuilder.config("spark.kubernetes.driver.pod.name", sparkProperties.getKubernetesDriverPodName());

// Activer l'allocation dynamique d'exécuteur (optionnel)
sparkBuilder.config("spark.dynamicAllocation.enabled", true);
sparkBuilder.config("spark.dynamicAllocation.shuffleTracking.enabled", true);
// Nombre d'exécuteurs minimum
sparkBuilder.config("spark.dynamicAllocation.minExecutors", 10);

// Connection à un datalake HDFS de type s3
sparkBuilder.config("spark.hadoop.fs.s3a.access.key", sparkProperties.getAccessKey());
sparkBuilder.config("spark.hadoop.fs.s3a.secret.key", sparkProperties.getSecretKey());
sparkBuilder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", sparkProperties.getSslEnabled());
sparkBuilder.config("spark.hadoop.fs.s3a.session.token", sparkProperties.getSessionToken());
sparkBuilder.config("spark.hadoop.fs.s3a.endpoint", sparkProperties.getSessionEndpoint());

// Transférer les dépendances Trevas aux exécuteurs Spark
// Voir https://stackoverflow.com/questions/28079307
sparkBuilder.config("spark.jars", String.join(",",
        "/vtl-spark.jar",
        "/vtl-model.jar",
        "/vtl-jackson.jar",
        "/vtl-parser.jar",
        "/vtl-engine.jar"
));

SparkSession spark = sparkBuilder.getOrCreate();
```

## Moteur d'exécution Trevas / Spark

Par défaut, un engine Trevas résoud les traitements `InMemory`.

Afin d'activer la résolution via Spark, deux propriétés sont à valoriser :

```java=
ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
ScriptContext context = engine.getContext();
context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
engine.put("$vtl.engine.processing_engine_names", "spark");
engine.put("$vtl.spark.session", spark);
```

## SparkDataset

Afin d'instancier un `SparkDataset` Trevas, une source de données Spark et un fichier json décrivant la structure des données sont requis.

### Source de données Spark

L'API Spark permet de créer des Dataset depuis plusieurs format : parquet, avro, json, csv, jdbc, ...

#### Parquet

```java=
Dataset<Row> dataset = spark.read().parquet("path/to/parquet/folder");
```

### Structure des données

#### Format

```json
[
  { "name": "id", "type": "STRING", "role": "IDENTIFIER" },
  { "name": "x", "type": "INTEGER", "role": "MEASURE" },
  { "name": "y", "type": "FLOAT", "role": "MEASURE" }
]
```

#### Sérialisation

```java=
List<Structured.Component> components =
objectMapper.readValue(Paths.get("path/to/structure.json")
                .toFile(),
        new TypeReference<>() {
        });
Structured.DataStructure structure = new Structured.DataStructure(components);
```

### Instanciation du `SparkDataset`

```java=
SparkDataset dataset = new SparkDataset(dataset, structure);
```
