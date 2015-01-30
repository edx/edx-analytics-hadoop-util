edx-analytics-hadoop-util
=========================

Building
--------

```bash
javac -cp $(hadoop classpath) org/edx/hadoop/input/ManifestTextInputFormat.java
jar cf edx-analytics-hadoop-util.jar org/edx/hadoop/input/ManifestTextInputFormat.class
```
