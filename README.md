# FlinkDynamicGraph
Dynamic Graph group Study TU Ilmenau

## Usage
1) Clone this project and import to Intelij IDEA
2) Run "maven compile" to download all dependencies
3) If SUCCESS, run App class main method to test it
4) Run "maven package" to create jar file with all dependencies
5) Download it to the server at your home directory:

```scp -P 2222 ~/IdeaProjects/FlinkDynamicGraph/target/flink-dynamic-graph-1.0-SNAPSHOT.jar  <your_username>@172.21.249.63:/home/<your_home_dir>```

6) Start execution on cluster:

```spark-submit --master yarn --class org.ilmenau.groupstudy.flinkdynamicgraph.app.App flink-dynamic-graph-1.0-SNAPSHOT.jar```

7) Edit project and push your changes
