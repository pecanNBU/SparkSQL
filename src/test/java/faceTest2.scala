import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/26.
  */
object faceTest2 {

  case class faceFeature(key: String, feature: String)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Hbase->Saprk->SQL")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 创建hbase configuration
    val hconf = HBaseConfiguration.create()
    val tableName = "photo_hbase1"
    hconf.set("hbase.zookeeper.quorum", "192.168.1.32")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.set(TableInputFormat.INPUT_TABLE, tableName)

    //从Hbase 获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //将数据映射为表，也就是将RDD转化为DataFrame
    val faceFeature = hbaseRDD.map(r => (
      r._1.toString, r._2.getValue("t1".getBytes, "p".getBytes)
    )).toDF("t1", "p")

    val faceFeature2 = hbaseRDD.map(r => (
      r._1.toString, r._2.getValue("t1".getBytes, "p".getBytes)
    )).toDF("t1", "p")

    //注册为零时表
    faceFeature.registerTempTable("face_feature")

    faceFeature.registerTempTable("face_feature2")

    // 从DataFrame查询出结果
    //val df2 = sqlContext.sql("SELECT * FROM face_feature as f1 cross join face_feature2 as f2 on f1.t1=f2.t1")
    val df2 = sqlContext.sql("SELECT * FROM face_feature as f1 cross join face_feature2 as f2")

    df2.show()

    /*val zz = hbaseRDD.map(a => {
      val key = a._1.toString
      val value = a._2
      val photo = value.getValue("t1".getBytes, "p".getBytes)
      (key, photo)
    }
    )

    zz.foreach(println)*/
    // zz.collect().foreach(println)

    /*print("================="+s)*/


    //    hbaseRDD.foreach{ case (_,result) =>
    //      //获取Rowkey
    //      val key =Bytes.toString(result.getRow)
    //      //通过列族和列名获取value
    //      val value= result.getValue("t1".getBytes,"p".getBytes)
    //      //将字节数组转换为字符串
    //      val v = Bytes.toString(value)
    //      println("key:"+key)
    //      println("length:"+value.length)
    //      println("value:"+v)
    //      println("length:"+v.length)
    //    }

    sc.stop()

  }


}
