from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#getOrCreate() 로 인스턴트 생성
spark = SparkSession.builder.appName("stream-word-count").getOrCreate()


lines_df = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()


#expr sql을 셀렉함수 안에서 쓸수있게 해주는 함수
words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
#밸류별로 그룹 나누고, 카운팅하기
counts_df = words_df.groupBy("word").count()

#콘솔로 내보내기
word_count_query = counts_df.writeStream.format("console")\
                            .outputMode("complete")\
                            .option("checkpointLocation", ".checkpoint")\
                            .start()
word_count_query.awaitTermination()