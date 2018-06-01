README
=========
Input arguments for jar: 
	(1) Platform<flink,spark,storm,kafka>
	(2) Benchmark Jar File
	(3) No. of Messages/Wave
	(4) Number of waves
	(5) No. of Producers
	(6) No. of Consumers
	(7) Kafka Replication Factor
	(8) Kafka Partitions
	(9) Message Type<small,medium,big,real>
   (10) Description (To add space use @, eg. this@is@a@benchmark)
   (11) Wave Interval

Run command: java -jar BenchmarkDriverMessages.jar (1) (2) (3) (4) (5) (6) (7) (8) (9) (10) (11)