edx-analytics-hadoop-util
=========================

Building Without Ant
--------------------

```bash
javac -cp $(hadoop classpath) org/edx/hadoop/input/ManifestTextInputFormat.java
jar cf edx-analytics-hadoop-util.jar org/edx/hadoop/input/ManifestTextInputFormat.class
```


Building With Ant
-----------------

To use the ANT build file you must compile using the correct version of Java, and move or copy the source files
in the `org` directory under a `src` directory.  You must also include the Hadoop jars that are specific to your
environment stored in a `lib` directory.  For Hadoop 2.7.2 this means compiling using Java 1.7 and the following jars:

- org.apache.hadoop:hadoop-common:2.7.2
- org.apache.hadoop:hadoop-mapreduce-client-core:2.7.2

Please note libraries are dependent upon the specific Hadoop version you are using.  Different versions of Hadoop may
require vastly different libraries or versions of Java.  You will need to consult your Hadoop implementation to
determine which libraries are appropriate for you situation.

