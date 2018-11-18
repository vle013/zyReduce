from __future__ import print_function

import sys
import re
import csv
import pyspark
from operator import add

from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark import SparkContext

save_location = "hdfs://10.0.4.2:8020/user/hadoop4//zyreduce/"
csv_location = save_location+"temp.folder"
file_location = save_location+'export.csv'

timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
act_header = Row("content_resource_id", "avg_time_spent", "avg_tries", "students_completed")
performance_header = Row("user_id", "content_resource_id", "start_time", "end_time", "seconds_taken")
student_header = Row("user_id", "mc_attempts", "sa_attempts", "cust_attempts", "avg_time_spent", "stdev_time_spent")

#def saveDfToCsv(df, tsvOutput, sep, header):
    #tmpParquetDir = "Posts.tmp.parquet"
  
    #df.repartition(1).write \
        #.format("com.databricks.spark.csv") \
        #.option("header", header.toString) \
        #.option("delimiter", sep) \
        #.save(tmpParquetDir)
  
    #dirc = new File(tmpParquetDir)
    #tmpTsvFile = tmpParquetDir + File.separatorChar + "part-00000"
    #(new File(tmpTsvFile)).renameTo(new File(tsvOutput))
  
    #dirc.listFiles.foreach( f => f.delete )
    #dirc.delete


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: zytabler <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder \
            .master('local[*]') \
            .appName('ZyReduceTabler') \
            .getOrCreate()

    df = spark.read \
            .csv(sys.argv[1], header=True, inferSchema=True)


# -------- PERFORMANCE TABLE --------

    timeWindow = Window.partitionBy('content_resource_id', 'user_id').orderBy('timestamp')
    compWindow = Window.partitionBy('content_resource_id', 'user_id').orderBy(f.desc('part'), f.desc('complete'))
    partWindow = Window.partitionBy('content_resource_id', 'user_id').orderBy(f.desc('part'))

    starts = df.withColumn('start_time', f.first('timestamp').over(timeWindow)) \
            .orderBy('user_id', 'content_resource_id', 'part', 'complete') \
            .where((df.complete == 0) & (df.part == 0))
    starts = starts.where(starts.start_time == starts.timestamp)

    ends = df.withColumn('end_time', f.first('timestamp').over(compWindow)) \
            .orderBy('user_id', 'content_resource_id', 'part', 'complete') \
            .drop('resource_type')
    ends = ends.where(ends.end_time == ends.timestamp)
    actPerformance = starts.join(ends, ['user_id', 'content_resource_id'], "inner") \
            .drop('timestamp').drop('part').drop('complete')
    actPerformance = actPerformance.withColumn('seconds_taken', \
            f.unix_timestamp('end_time', format=timeFmt) \
            - f.unix_timestamp('start_time', format=timeFmt)) \
            .orderBy('user_id') \
            .dropDuplicates()
    

# -------- STUDENT TABLE --------

    mc = df.withColumn('val', f.when(df.resource_type == 'multiple_choice', 1).otherwise(0)) \
            .groupBy('user_id') \
            .agg(f.sum('val').alias('mc_attempts'))

    sa = df.withColumn('val', f.when(df.resource_type == 'short_answer', 1).otherwise(0)) \
            .groupBy('user_id') \
            .agg(f.sum('val').alias('sa_attempts'))

    cust = df.withColumn('val', f.when(df.resource_type == 'custom', 1).otherwise(0)) \
            .groupBy('user_id') \
            .agg(f.sum('val').alias('cust_attempts'))

    mn_spt = actPerformance.groupBy('user_id') \
            .agg(f.avg('seconds_taken').alias('avg_activity_time'))
    std_spt = actPerformance.groupBy('user_id') \
            .agg(f.stddev('seconds_taken').alias('stddev_activity_time'))

    student_df = mc.join(sa, "user_id", "inner").join(cust, "user_id", "inner") \
            .join(mn_spt, "user_id", "inner").join(std_spt, "user_id", "inner") \
            .orderBy('user_id') \
            .dropDuplicates()

# -------- ACTIVITY TABLE --------

    avg_spent = actPerformance.groupBy('content_resource_id') \
            .agg(f.avg('seconds_taken').alias('avg_time'))

    avg_tries = df.groupBy('user_id', 'content_resource_id').count()
    avg_tries = avg_tries.groupBy('content_resource_id') \
            .agg(f.avg('count').alias('avg_tries'))

    stds_comp = df.where(df.complete == 1) \
            .withColumn('val', f.first('user_id').over(partWindow))
    stds_comp = stds_comp.groupBy('content_resource_id', 'user_id') \
            .count().groupBy('content_resource_id').count()

    act_df = avg_spent.join(df.select('content_resource_id', 'resource_type') \
            , 'content_resource_id', "inner") \
            .join(avg_tries, "content_resource_id", "inner") \
            .join(stds_comp, "content_resource_id", "inner") \
            .orderBy('content_resource_id') \
            .dropDuplicates()

# -------- CSV WRITING --------

    #with open('zyreduce_activity.csv', 'w') as fileA:
        #writerA = csv.writer(fileA)
        #writerA.writerow(act_header)
        #writerA.writerows(act_df.collect())
    
    #student_df.show(1000)
    #actPerformance.show(10000)

    #student_df.write.csv(save_location+'zyreduce_student.csv', mode='overwrite', header=True)
    #actPerformance.write.csv(save_location+'zyreduce_performance.csv', mode='overwrite', header=True)
    act_df.write \
            .csv(save_location+'zyreduce_activity', mode='overwrite', header=True)
    #file = dbutils.fs.ls(csv_location)[-1].path
    #dbutils.fs.cp(file, file_location)
    #dbutils.fs.rm(csv_location, recurse=True)
    
    #student_df.coalesce(1).write.csv(save_location+'zyreduce_student', mode='overwrite', header=True)
    #student_df.toPandas().to_csv(save_location+'zyreduce_student.csv')
    #student_df.show(117)

    #print(actPerformance.collect()) 
