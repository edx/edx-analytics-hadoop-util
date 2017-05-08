edx-analytics-hadoop-util
=========================

Building
--------

```bash
javac -cp $(hadoop classpath) src/org/edx/hadoop/input/org.edx.hadoop.input.ManifestTextInputFormat.java
jar cf edx-analytics-hadoop-util.jar src/org/edx/hadoop/input/org.edx.hadoop.input.ManifestTextInputFormat.class
```


Ant File Usage
--------------

To use the ANT build file you must have the following libraries imported
- org.apache.hadoop:hadoop-common:2.7.2
- org.apache.hadoop:hadoop-mapreduce-client-core:2.7.2

and available in a folder titled `lib`.  This jar has to be built and compiled using java 1.7