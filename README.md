# Spark-Example
com.zxl.spark2_2.kafka 
	
	StreamingKafka8：
		
		SparkStreaming从kafka中读取数据
  		
		kafka版本0.8
  		
		采取直连方式
  	
	StreamingKafka10：
  		
		SparkStreaming从kafka中读取数据
  		
		kafka版本0.10
  		
		采取直连方式

 com.zxl.spark2_2.streaming 
 	
	StreamingToMysql：
 		
		SparkStreaming读取数据，存储到Mysql中

 com.zxl.spark2_2.structured 
 	
	JDBCSink：
 		
		处理从StructuredStreaming中向mysql中写入数据
 	
	MySqlPool：
 		
		从mysql连接池中获取连接
 	
	StructuredStreamingKafka：
 		
		结构化流从kafka中读取数据存储到关系型数据库mysql
  		
		目前结构化流对kafka的要求版本0.10及以上 

com.zxl.spark2_2.dataset

	createDataSet：
	
		DataSet创建的多种方式
		
	basicAction：
	
		DataSet的基本操作
		
	actions：
	
		DataSet的Action操作

com.zxl.spark1_6.dataframe
	
	SQLDemo：
		
		从hdfs中读取数据，转化为DataFrame，执行简单操作

com.zxl.spark1_6.elastic 
	
	ElasticSpark：
		
		Elasticsearch是一个基于Lucene的实时地分布式搜索和分析引擎。
  		
		设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。

com.zxl.spark1_6.flume
	
	FlumePushWordCount：
		
		flume向spark发送数据
  		
		添加三个jar包
  			
			- commons-lang3-3.3.2.jar
  			
			- scala-library-2.10.5.jar
  			
			- spark-streaming-flume-sink_2.10-1.6.1.jar
  		
		打成jar包上传到集群中运行
  		
		集群命令如下：
  		
		bin/spark-submit --master spark://node1:7077 --class com.zxl.spark1_6.flume.FlumePushWordCount /jar/____.jar 192.168.13.131 8888

com.zxl.spark1_6.jedis 
	
	JedisConnectionPool：
		
		获得Jedis连接，进行简单操作

com.zxl.spark1_6.kafka 
	
	DirectKafkaWordCount：
		
		Spark Streaming维护偏移量相关的信息，实现零数据丢失，保证不重复消费
  		
		采用直连的方式有一个缺点，就是不再向zookeeper中更新offset信息。
  		
		因此，在采用直连的方式消费kafka中的数据的时候，大体思路是首先获取保存在zookeeper中的偏移量信息，
  		
		根据偏移量信息去创建stream，消费数据后再把当前的偏移量写入zookeeper中
 		
		在2.0以前的版本中KafkaManager这个类是private权限，需要把它拷贝到项目里使用。
  			org.apache.spark.streaming.kafka
  	
	KafkaWordCount：
  		
		从集群中的kafka读取数据操作
  		
		运行时参数：
  			
			node1:2181,node2:2181,node3:2181 g1 test 2
  			
			其中g1为组名，此处随意写，test为topic名，kafka中的topic名要一致
  		
		集群命令(需先启动完成)：
        	
		1.启动kafak
         		
			bin/kafka-server-start.sh config/server.properties > /dev/null 2>&1 &
  	     	
		2.创建topic
        		
			bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic test
  		
		3.向topic中添加数据
 			
			bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

com.zxl.spark1_6.my_partitioner 
	
	UrlCountPartition：
		
		自定义分区
  		
		数据格式(时间点  url地址)，例如：
  			20160321101954	http://net.zxl.cn/net/video.shtml
  		
		处理成数据(k, v)
  		
		对于数据(k, v)
  		
		重写自己的 partitioner

com.zxl.spark1_6.my_sort 
	
	CustomSort：自定义排序

com.zxl.spark1_6.mysql 
	
	JdbcRDDDemo：简单连接数据库操作

com.zxl.spark1_6.simple 
	
	AdvUrlCount：
		
		读取文本内容,根据指定的学科, 取出点击量前三的
  		
		文本内容为某广告链接点击量，格式为：(时间点  某学科url链接)
  		
		举例：(20160321101957	http://net.zxl.cn/net/course.shtml)
  	
	IpDemo：
  		
		数据格式如下：
  			(1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302)
  		
		根据ip地址转换为数字，从数据集中找出详细信息.
  		
		为了简化查找速率，采用二分查找.
  	
	UserLocation：
  		
		根据日志统计出每个用户在站点所呆时间最长的前2个的信息
  		
		日志内容格式为(手机号,时间点,基站站点,事件类型),事件类型为1时是进入基站,0是出基站。
  			
			1, 先根据"手机号_站点"为唯一标识, 算一次进站出站的时间, 返回(手机号_站点, 时间间隔)
  			
			2, 以"手机号_站点"为key, 统计每个站点的时间总和, ("手机号_站点", 时间总和)
  			
			3, ("手机号_站点", 时间总和) --> (手机号, 站点, 时间总和)
  			
			4, (手机号, 站点, 时间总和) --> groupBy().mapValues(以时间排序,取出前2个) --> (手机->((m,s,t)(m,s,t)))
  	
	WordCount：
  		
		简单WordCount实现
  		
		集群上执行示例，指定相关配置
  		
		bin/spark-submit --master spark://node1:7077 --class com.zxl.spark1_6.simple.WordCount --executor-memory 512m	--total-executor-cores 2 /opt/soft/jar/hello-spark-1.0.jar hdfs://node1:9000/wc hdfs://node1:9000/out

com.zxl.spark1_6.streaming
	
	LoggerLevels：
		
		设置打印的log的级别
	
	StateFulWordCount：
		
		Spark Streaming累加器操作（updateStateByKey)
	
	StreamingWordCount：
		
		通过SparkStreaming简单实现WordCount
	
	WindowOpts：
		
		SparkStreaming窗口函数的实现

org.apache.spark.streaming.kafka 
	
	KafkaManager：
		
		SparkStreaming直连kafka获取数据，自己编写偏移量offset，用于spark2.0以前
