#!/bin/bash

export PIG_CLASSPATH=/home/hadoop/pig/lib
export PIG_CONF_DIR=/home/hadoop/pig/conf
export PIG_HOME=/home/hadoop/pig
export YARN_CONF_DIR=/home/hadoop/conf
export YARN_HOME=/home/hadoop
export JAVA_HOME=/usr/java/latest/
export CASCADING_SDK_HOME=/home/hadoop/cascading
export HADOOP_COMMON_HOME=/home/hadoop
export HADOOP_CONF_DIR=/home/hadoop/conf
export HADOOP_HDFS_HOME=/home/hadoop
export HADOOP_HOME=/home/hadoop
export HADOOP_HOME_WARN_SUPPRESS=true
export HADOOP_MAPRED_HOME=/home/hadoop
export HADOOP_PREFIX=/home/hadoop
export HADOOP_YARN_HOME=/home/hadoop
export HBASE_CONF_DIR=/home/hadoop/hbase/conf
export HBASE_HOME=/home/hadoop/hbase
export HIVE_CONF_DIR=/home/hadoop/hive/conf
export HIVE_HOME=/home/hadoop/hive
export IMPALA_CONF_DIR=/home/hadoop/impala/conf
export IMPALA_HOME=/home/hadoop/impala
export LD_LIBRARY_PATH=/home/hadoop/lib/native:/usr/lib64:/usr/local/cuda/lib64:/usr/local/cuda/lib:
export MAHOUT_CONF_DIR=/home/hadoop/mahout/conf
export MAHOUT_HOME=/home/hadoop/mahout
#export PATH=/home/hadoop/hbase/bin:/home/hadoop/pig/bin:/usr/local/cuda/bin:/usr/java/latest/bin:/home/hadoop/bin:/home/hadoop/mahout/bin:/home/hadoop/hive/bin:/home/hadoop/hbase/bin:/home/hadoop/impala/bin:/home/hadoop/spark/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin:/home/hadoop/cascading/tools/multitool-20140224/bin:/home/hadoop/cascading/tools/load-20140223/bin:/home/hadoop/cascading/tools/lingual-client/bin:/home/hadoop/cascading/driven/bin
export PIG_CLASSPATH=/home/hadoop/pig/lib
export PIG_CONF_DIR=/home/hadoop/pig/conf
export PIG_HOME=/home/hadoop/pig
export SPARK_CONF_DIR=/home/hadoop/spark/conf
export SPARK_HOME=/home/hadoop/spark
export YARN_CONF_DIR=/home/hadoop/conf
export YARN_HOME=/home/hadoop
export PATH=/home/hadoop/hbase/bin:/home/hadoop/pig/bin:/usr/local/cuda/bin:/usr/java/latest/bin:/home/hadoop/bin:/home/hadoop/mahout/bin:/home/hadoop/hive/bin:/home/hadoop/hbase/bin:/home/hadoop/impala/bin:/home/hadoop/spark/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin:/home/hadoop/cascading/tools/multitool-20140224/bin:/home/hadoop/cascading/tools/load-20140223/bin:/home/hadoop/cascading/tools/lingual-client/bin:/home/hadoop/cascading/driven/bin


if [[ -z "$@" ]]; then
    echo >&2 "You must supply the S3 bucket name!"
    exit 1
fi

ERROR_COUNT=0;
BACK_TIME="$(date +'%Y_%m_%d_%H_%M')"
echo $BACK_TIME


OUTPUT="output";
FINAL_OUTPUT="output";
SOURCE="Exception";


echo "Snapshot location in S3 bucket: $1"; 
S3_BUCKET="s3://$1/backup/EMR/snapshot/";
echo $S3_BUCKET;

mkdir -p /home/hadoop/logs/

echo "Starting job at $BACK_TIME" >> /home/hadoop/logs/$BACK_TIME.log 2>&1
PIDFILE=/home/hadoop/job.pid
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Process already running"
    exit 1
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
      exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo "Could not create PID file"
    exit 1
  fi
fi


#Real code for backup
echo "Backup Started"

declare -a TABLES=(Account AccountAliasMap Consignment ConsignmentItem ConsignmentItemDangerousGoods ConsignmentItemDimensions ConsignmentProcessedView ConsignmentRouting Eventstate GeneratedUserNotifications RawEvents References TDFDirtyConsignment TGXDirtyConsignment TWConsignmentHashMap User UserConsignmentNotification UserWatchedConsignment);
for TABLE in "${TABLES[@]}"
do
        echo "Going to take backup of " $TABLE

        echo snapshot \'$TABLE\' , \'snap_$TABLE\' | hbase shell >> /home/hadoop/logs/$BACK_TIME.log 2>&1
		sleep 10s
        hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -snapshot snap_$TABLE  -copy-to hdfs:///hbasebackup/$TABLE/ >> /home/hadoop/logs/$BACK_TIME.log 2>&1
        sleep 10s
        hadoop distcp hdfs:///hbasebackup/$TABLE $S3_BUCKET$BACK_TIME/$TABLE >> /home/hadoop/logs/$BACK_TIME.log 2>&1

        if cat /home/hadoop/logs/$BACK_TIME.log | grep -Ei "Exception|Error"; then
         ERROR_COUNT=$(($ERROR_COUNT+1));
		 sleep 10s
         echo $ERROR_COUNT >> /home/hadoop/logs/$BACK_TIME.log 2>&1
		  echo delete_snapshot \'snap_$TABLE\' | hbase shell >> /home/hadoop/logs/$BACK_TIME.log 2>&1
         break;
        fi
        sleep 10s
         echo delete_snapshot \'snap_$TABLE\' | hbase shell
		sleep 10s

done


if test $ERROR_COUNT -gt 0; then
        echo "Error found while taking backup, deleting the current backup at $BACK_TIME" >> /home/hadoop/logs/$BACK_TIME.log 2>&1
        aws s3 rm $S3_BUCKET$BACK_TIME/ --recursive >> /home/hadoop/logs/$BACK_TIME.log 2>&1

fi
aws s3 cp /home/hadoop/logs/$BACK_TIME.log $S3_BUCKET$BACK_TIME.log
rm /home/hadoop/logs/$BACK_TIME.log
rm $PIDFILE

