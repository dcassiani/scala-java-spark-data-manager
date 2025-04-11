# scala-java-spark-data-manager
base de Scala Java para Spark Cloudera - estagio de transformacao de ETL 

# REFs
	- https://docs.scala-lang.org/tour/traits.html
	- https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures-
	- https://spark.apache.org/docs/3.5.0/sql-ref-syntax-ddl-alter-table.html
	- https://spark.apache.org/docs/3.5.0/sql-performance-tuning.html
	- https://hbase.apache.org/book.html#datamodel
	- https://archive.apache.org/dist/spark/docs/1.6.0/programming-guide.html
	- https://www.elastic.co/guide/en/elasticsearch/reference/6.5/docs-update.html
	- https://community.cloudera.com/t5/Community-Articles/HBase-Spark-in-CDP/ta-p/294868
	
	
# ==================== config intelliJ Scala
clicar com direito no projeto > Open Module Settings (F4) > Project = {
	Project SDK = 1.4 version 14.0.2 /ou correto-1.8
	Project Language Level = 14 (switch expressions) (SDK DEFAULT)
	}
> Plataform Settings > SDKs = correto-1.8

File > Settings > Build, Execution, Deployment >
	> Build Tools > sbt > JRE = correto-1.8 /ou correto-1.8
	> Compiler > Scala Compiler > Scala Compile Server > JDK = 1.4 version 14.0.2 /ou correto-1.8 + -server -Xss2m -XX:+UseParallelGC -XX:MaxInlineLevel=20
	
console - SBT > clean > compile > assembly > JAR no TARGET