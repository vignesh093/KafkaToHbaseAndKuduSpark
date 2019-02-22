package com.spark.hbaseload

import java.text.SimpleDateFormat

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.SQLContext
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.client.CreateTableOptions
import scala.collection.JavaConverters._
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduClient
import java.util.ArrayList
import org.apache.kudu.ColumnSchema
import org.apache.kudu.Schema
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable
import org.apache.kudu.client.KuduSession
import org.apache.kudu.client.Insert

case class ROSdata(seq: Double, secs: Double, nsecs: Double, orientation_x: Double, orientation_y: Double, orientation_z: Double,
                   angular_velocity_x: Double, angular_velocity_y: Double, angular_velocity_z: Double, linear_acceleration_x: Double, linear_acceleration_y: Double, linear_acceleration_z: Double)

object LoadRosBagdata {

  val simpleformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS") //dateformat for eventtime

  val rosbagschema = (new StructType)
    .add("seq", DoubleType)
    .add("secs", DoubleType)
    .add("nsecs", DoubleType)
    .add("orientation_x", DoubleType)
    .add("orientation_y", DoubleType)
    .add("orientation_z", DoubleType)
    .add("angular_velocity_x", DoubleType)
    .add("angular_velocity_y", DoubleType)
    .add("angular_velocity_z", DoubleType)
    .add("linear_acceleration_x", DoubleType)
    .add("linear_acceleration_y", DoubleType)
    .add("linear_acceleration_z", DoubleType)

  def main(args: Array[String]): Unit = {

    val CF_COLUMN_NAME = Bytes.toBytes("FCA")

    val spark = SparkSession
      .builder
      .appName("LoadRosBagdata")
      //.master("local[*]")
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "2") //neccessary otherwise in local mode job keeps running without output

    import spark.implicits._

    val sqlContext = new SQLContext(spark.sparkContext)
    //val kuduContext = new KuduContext("adasdemo1:7051", spark.sparkContext)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "adasdemo1:9092,adasdemo2:9092,adasdemo3:9092")
      .option("subscribe", "hello2")
      .load()

    val KafkaDS = df.selectExpr("CAST(key as STRING)", "CAST(value as STRING)").as[(String, String)]

    val RosdataDF = KafkaDS.select(from_json('value, rosbagschema) as 'json_data)

    val RosdataDS = RosdataDF.select(RosdataDF("json_data.seq"), RosdataDF("json_data.secs"), RosdataDF("json_data.nsecs"),
      RosdataDF("json_data.orientation_x"), RosdataDF("json_data.orientation_y"), RosdataDF("json_data.orientation_z"), RosdataDF("json_data.angular_velocity_x"),
      RosdataDF("json_data.angular_velocity_y"), RosdataDF("json_data.angular_velocity_z"), RosdataDF("json_data.linear_acceleration_x"),
      RosdataDF("json_data.linear_acceleration_y"), RosdataDF("json_data.linear_acceleration_z")).as[ROSdata].filter(data => (data.seq >= 1588858.0 && data.seq < 1589100.0))

    //hbase sink
    val hbasewriter = new ForeachWriter[ROSdata] {
      var rosbagrecords = new ListBuffer[Put]
      var table: Table = null
      var connection: Connection = null

      def open(partitionId: Long, version: Long): Boolean = {
        val zkQuorum = "adasdemo1,adasdemo2,adasdemo3"
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zkQuorum)
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        connection = ConnectionFactory.createConnection(conf)
        table = connection.getTable(TableName.valueOf(Bytes.toBytes("FCA_DATA")))
        true
      }

      def process(data: ROSdata): Unit = {
        println(data.seq + "," + data.secs + "," + data.orientation_x + "," + data.angular_velocity_x)
        val p = new Put(Bytes.toBytes(data.seq.toString()))
        //{"seq":1603209,"secs":1479425475,"nsecs":983179092,"orientation_x":-0.0198964538567,"orientation_y":-0.00946842788834,"orientation_z":-0.520565836366,"angular_velocity_x":0.0,"angular_velocity_y":0.0,"angular_velocity_z":0.0,"linear_acceleration_x":0.200660020113,"linear_acceleration_y":0.785413205624,"linear_acceleration_z":7.96111392975}
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("SECS"), Bytes.toBytes(data.secs.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("NSECS"), Bytes.toBytes(data.nsecs.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("ORIENTATION_X"), Bytes.toBytes(data.orientation_x.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("ORIENTATION_Y"), Bytes.toBytes(data.orientation_y.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("ORIENTATION_Z"), Bytes.toBytes(data.orientation_z.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("ANGULAR_VELOCITY_X"), Bytes.toBytes(data.angular_velocity_x.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("ANGULAR_VELOCITY_Y"), Bytes.toBytes(data.angular_velocity_y.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("ANGULAR_VELOCITY_Z"), Bytes.toBytes(data.angular_velocity_z.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("LINEAR_ACCELERATION_X"), Bytes.toBytes(data.linear_acceleration_x.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("LINEAR_ACCELERATION_Y"), Bytes.toBytes(data.linear_acceleration_y.toString()))
        p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("LINEAR_ACCELERATION_Z"), Bytes.toBytes(data.linear_acceleration_z.toString()))

        rosbagrecords += (p)

      }
      def close(errorOrNull: Throwable): Unit = {
        table.put(rosbagrecords.asJava)
        table.close
        connection.close
      }
    }

    /*    val kuduTableOptions = new CreateTableOptions()
kuduTableOptions.
 setRangePartitionColumns(List("seq").asJava).
 setNumReplicas(3)

    if (!kuduContext.tableExists(kuduTableName)) {
 kuduContext.createTable(
 // Table name, schema, primary key and options
 kuduTableName, RosdataDS.schema, Seq("seq"), kuduTableOptions)
}
   */

    // kuduContext.insertRows(RosdataDS.toDF(), kuduTableName)

    //hbase sink
    var table: KuduTable = null
    var connection: Connection = null
    var client: KuduClient = null
    var session: KuduSession = null
    val kuduwriter = new ForeachWriter[ROSdata] {

      def open(partitionId: Long, version: Long): Boolean = {
        /* val kuduTableOptions = new CreateTableOptions()
        kuduTableOptions.
          setRangePartitionColumns(List("seq").asJava).
          setNumReplicas(3)

        if (!kuduContext.tableExists(kuduTableName)) {
          kuduContext.createTable(
            // Table name, schema, primary key and options
            kuduTableName, RosdataDS.schema, Seq("seq"), kuduTableOptions)
        }*/

        client = new KuduClient.KuduClientBuilder("adasdemo1").build()

        var kuduTableName = "FCA_DATA_KUDU"

        if (!client.tableExists(kuduTableName)) {

          val seqCol = new ColumnSchemaBuilder("SEQ", Type.INT64).key(true).build()
          val secsCol = new ColumnSchemaBuilder("SECS", Type.DOUBLE).build()
          val nsecsCol = new ColumnSchemaBuilder("NSECS", Type.DOUBLE).build()
          val ore_x = new ColumnSchemaBuilder("ORIENTATION_X", Type.DOUBLE).build()
          val ore_y = new ColumnSchemaBuilder("ORIENTATION_Y", Type.DOUBLE).build()
          val ore_z = new ColumnSchemaBuilder("ORIENTATION_Z", Type.DOUBLE).build()
          val ang_x = new ColumnSchemaBuilder("ANGULAR_VELOCITY_X", Type.DOUBLE).build()
          val ang_y = new ColumnSchemaBuilder("ANGULAR_VELOCITY_Y", Type.DOUBLE).build()
          val ang_z = new ColumnSchemaBuilder("ANGULAR_VELOCITY_Z", Type.DOUBLE).build()
          val lin_x = new ColumnSchemaBuilder("LINEAR_ACCELERATION_X", Type.DOUBLE).build()
          val lin_y = new ColumnSchemaBuilder("LINEAR_ACCELERATION_Y", Type.DOUBLE).build()
          val lin_z = new ColumnSchemaBuilder("LINEAR_ACCELERATION_Z", Type.DOUBLE).build()

          val columns = new ArrayList[ColumnSchema]()
          columns.add(seqCol)
          columns.add(secsCol)
          columns.add(nsecsCol)
          columns.add(ore_x)
          columns.add(ore_y)
          columns.add(ore_z)
          columns.add(ang_x)
          columns.add(ang_y)
          columns.add(ang_z)
          columns.add(lin_x)
          columns.add(lin_y)
          columns.add(lin_z)

          val schema = new Schema(columns);

          val kuduTableOptions = new CreateTableOptions()
          kuduTableOptions.
            setRangePartitionColumns(List("SEQ").asJava).
            setNumReplicas(3)

          client.createTable(kuduTableName, schema, kuduTableOptions)
        }

        table = client.openTable(kuduTableName);
        session = client.newSession();

        true
      }

      def process(data: ROSdata): Unit = {
        /* val RosDatastr = data.toString
          try{
          var insert = table.newInsert();
          var row = insert.getRow();

          //println(RosDatastr)
          val rosdata = RosDatastr.substring(RosDatastr.indexOf("(")+1,RosDatastr.indexOf(")")).split(",")
          var count = 0
          //println(rosdata)
          for(ind_data <- rosdata){
            if(count == 0)
              row.addLong(count, ind_data(count).toLong)
            else
              row.addDouble(count, ind_data(count))
            count = count + 1
          }

          session.apply(insert)
          }
          catch{
            case e: StringIndexOutOfBoundsException => println("RosDatastr "+RosDatastr)
          }*/
        var insert = table.newInsert();
        var row = insert.getRow();
        row.addLong("SEQ", data.seq.toLong)
        row.addDouble("SECS", data.secs)
        row.addDouble("NSECS", data.nsecs)
        row.addDouble("ORIENTATION_X", data.orientation_x)
        row.addDouble("ORIENTATION_Y", data.orientation_y)
        row.addDouble("ORIENTATION_Z", data.orientation_z)
        row.addDouble("ANGULAR_VELOCITY_X", data.angular_velocity_x)
        row.addDouble("ANGULAR_VELOCITY_Y", data.angular_velocity_y)
        row.addDouble("ANGULAR_VELOCITY_Z", data.angular_velocity_z)
        row.addDouble("LINEAR_ACCELERATION_X", data.linear_acceleration_x)
        row.addDouble("LINEAR_ACCELERATION_Y", data.linear_acceleration_y)
        row.addDouble("LINEAR_ACCELERATION_Z", data.linear_acceleration_z)

        val inserted = session.apply(insert)
        //println("Message "+ inserted.getRowError.getErrorStatus.toString())

      }
      def close(errorOrNull: Throwable): Unit = {
        session.close()
        client.shutdown()
      }
    }
    
   val hbasesink = RosdataDS
      .writeStream
      .outputMode("update")
      .foreach(hbasewriter)
      .start()
      
    val kudusink = RosdataDS
      .writeStream
      .outputMode("update")
      .foreach(kuduwriter)
      .start()

    hbasesink.awaitTermination()
    kudusink.awaitTermination()

  }
}

