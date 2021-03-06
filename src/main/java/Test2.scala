import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/26.
  */
object Test2 {

  case class faceFeature(key: String, feature: String)

  def main(args: Array[String]) {
    /*val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Hbase->Saprk->SQL")
      .config("spark.some.config.option", "some value")
      .getOrCreate()
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")*/
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Hbase->Saprk->SQL")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    //System.setProperty("hadoop.home.dir", "F:\\software\\hadoop-2.7.3");

    /* val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Hbase->Saprk->SQL")
     sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")*/

    //    val scRDD = new SparkContext(sparkConf)

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
      //Bytes.toString(r._2.getValue(Bytes.toBytes("t1"),Bytes.toBytes("p"))),
      r._1.toString, r._2.getValue("t1".getBytes, "p".getBytes)
      //Bytes.toString(r._2.getValue(Bytes.toBytes("t1"),Bytes.toBytes("p")))
    )).toDF("t1", "p")
    //注册为临时表
    faceFeature.registerTempTable("face_feature")

    // 从DataFrame查询出结果
    val df2 = sqlContext.sql("SELECT t1,p FROM face_feature")

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
