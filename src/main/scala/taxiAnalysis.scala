import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.collection.mutable.ListBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level

object taxiAnalysis {

  case class Ride(pickupDate: Long, dropoffDate: Long, pickupMonth: Int, pickupDay: Int,
                  pickupDayOfWeek: String, pickupHour: Int, passengerCount: Int,
                  km: Float, rateCodeID: Int, paymentType: Int, fareAmount: Float,
                  extra: Float, tipAmount: Float, totalAmount: Float,
                  pickupLocation: Int, dropoffLocation: Int)

  def main(args: Array[String]) {

    // Disable logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Check input parameters
    if (args.length < 2) {
      System.err.println("Wrong number of parameters. Usage: ")
      System.err.println("<Input file or directory>")
      System.err.println("<Output directory>")
      System.exit(1)
    }

    val fileIn = args(0)
    val folderOut = args(1)
    var timeLogs = new ListBuffer[String]()

    // Creation of Spark Session
    val spark = SparkSession.builder()
      .appName("taxiAnalysis")
      .enableHiveSupport()  
      .getOrCreate()

    import spark.implicits._

    // Read file(s)
    val file = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(fileIn)

    file.createOrReplaceTempView("taxi")

    // Data cleaning and DateTime conversions using SQL
    val ridesDF = spark.sql("SELECT UNIX_TIMESTAMP(tpep_pickup_datetime) pickup, " +
      "UNIX_TIMESTAMP(tpep_dropoff_datetime) dropoff, " +
      "MONTH(tpep_pickup_datetime) pickup_month, " +
      "DAY(tpep_pickup_datetime) pickup_day, " +
      "DATE_FORMAT(tpep_pickup_datetime, 'EEEE') pickup_day_of_week, " +
      "HOUR(tpep_pickup_datetime) pickup_hour, " +
      "Passenger_count, CAST(Trip_distance * 1.60934 AS FLOAT) AS km, " +
      "RatecodeID, Payment_type, CAST(Fare_amount AS FLOAT) AS Fare_amount, " +
      "CAST(Extra AS FLOAT) Extra, CAST(Tip_amount AS FLOAT) Tip_amount, " +
      "CAST(Total_amount AS FLOAT) Tip_amount, PULocationID, DOLocationID " +
      "FROM taxi " +
      "WHERE tpep_pickup_datetime IS NOT NULL " +
      "AND tpep_dropoff_datetime IS NOT NULL " +
      "AND Trip_distance IS NOT NULL " +
      "AND Trip_distance >= 1 " +
      "AND RatecodeID IS NOT NULL " +
      "AND RatecodeID BETWEEN 1 AND 6 " +
      "AND UNIX_TIMESTAMP(tpep_dropoff_datetime) > 0 " +
      "AND UNIX_TIMESTAMP(tpep_pickup_datetime) > 0 " +
      "AND UNIX_TIMESTAMP(tpep_dropoff_datetime) > unix_timestamp(tpep_pickup_datetime)")

    // Conversion to RDDs to make analysis
    val ridesRDD = ridesDF.map(x => Ride(x.getLong(0), x.getLong(1), x.getInt(2), x.getInt(3),
      x.getString(4), x.getInt(5), x.getInt(6), x.getFloat(7), x.getInt(8),
      x.getInt(9), x.getFloat(10), x.getFloat(11), x.getFloat(12), x.getFloat(13),
      x.getInt(14), x.getInt(15)))
      .rdd
      .cache()

    // Analysis 1: Average speed per hour
    time(averageSpeedPerHour(spark, ridesRDD, folderOut), timeLogs, "averageSpeedPerHour")

    // Analysis 2: Average income per working day and week end
    time(averageAmountPerDay(spark, ridesRDD, folderOut), timeLogs, "averageAmountPerDay")

    // Analysis 3: 10 Most distant places measured in minutes
    time(mostDistantPlaces(spark, ridesRDD, folderOut), timeLogs, "mostDistantPlaces")

    // Analysis 4: Average speed per day of week and day/night hour
    time(averageSpeedPerDay(spark, ridesRDD, folderOut), timeLogs, "averageSpeedPerDay")

    // Analysis 5: Monthly distance and incomes per rate type
    time(monthlyTotalsPerRate(spark, ridesRDD, folderOut), timeLogs, "monthlyTotalsPerRate")

    // Print all time logs
    timeLogs.foreach(println)
  }

  def averageSpeedPerHour(spark: SparkSession, ridesRDD: RDD[Ride], folderOut: String): Unit = {

    val rdd = ridesRDD.map(x => (x.pickupHour, (x.km * 3600 / (x.dropoffDate-x.pickupDate), 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .filter(_._2._2 > 0)
      .mapValues(x => x._1 / x._2)
      .sortByKey()

    import spark.implicits._

    rdd.toDF
      .write
      .option("sep",";")
      .mode(SaveMode.Overwrite)
      .csv(folderOut + "AverageSpeedPerHour")
  }

  def averageAmountPerDay(spark: SparkSession, ridesRDD: RDD[Ride], folderOut: String): Unit = {

    val rdd = ridesRDD.map(x =>
      if (x.pickupDayOfWeek == "Monday" ||
          x.pickupDayOfWeek == "Tuesday" ||
          x.pickupDayOfWeek == "Wednesday" ||
          x.pickupDayOfWeek == "Thursday" ||
          x.pickupDayOfWeek == "Friday")
        ("WorkingDay", (x.totalAmount, 1))
      else
        ("WeekEnd", (x.totalAmount, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .filter(_._2._2 > 0)
      .mapValues(x => x._1 / x._2)

    import spark.implicits._

    rdd.toDF
      .write
      .option("sep",";")
      .mode(SaveMode.Overwrite)
      .csv(folderOut + "AverageAmountPerDayType")
  }

  def mostDistantPlaces(spark: SparkSession, ridesRDD: RDD[Ride], folderOut: String): Unit = {

    val rdd = ridesRDD.map(x =>
      ("From: " + x.pickupLocation + " To: " + x.dropoffLocation, (x.dropoffDate - x.pickupDate).toInt))
      .aggregateByKey((0,0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .filter(_._2._2 > 0)
      .mapValues(x => x._1.toFloat / x._2 / 60)
      .sortBy(_._2, false)

    import spark.implicits._

    rdd.toDF
      .limit(20)
      .write
      .option("sep",";")
      .mode(SaveMode.Overwrite)
      .csv(folderOut + "MostDistantPlaces")
  }

  def averageSpeedPerDay(spark: SparkSession, ridesRDD: RDD[Ride], folderOut: String): Unit = {

    val days = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

    val rdd = ridesRDD.map(x => {
      val timeOfDay = if (x.pickupHour >= 8 && x.pickupHour <= 20) "Day" else "Night"
      val prevDayIndex = if (days.indexOf(x.pickupDayOfWeek) == 0) 6 else days.indexOf(x.pickupDayOfWeek)-1
      val dayOfWeek = if (x.pickupHour > 20) x.pickupDayOfWeek else days(prevDayIndex)

      (dayOfWeek + " " + timeOfDay, x.km * 3600 / (x.dropoffDate - x.pickupDate))
    })
      .aggregateByKey((0.0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .filter(_._2._2 > 0)
      .mapValues(x => x._1 / x._2)
      .sortByKey()

    import spark.implicits._

    rdd.toDF
      .write
      .option("sep",";")
      .mode(SaveMode.Overwrite)
      .csv(folderOut + "AverageSpeedPerDay")
  }

  def monthlyTotalsPerRate(spark: SparkSession, ridesRDD: RDD[Ride], folderOut: String): Unit = {

    val months = List("January","February","March","April","May","June","July","August","September","October","November","December")
    val rateTypes = List("Standard rate","JFK","Newark","Nassau or Westchester","Negotiated fare","Group ride")

    val rdd = ridesRDD.map(x => (x.pickupMonth + " " + x.rateCodeID, (x.km, x.totalAmount)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => {
        val key = x._1.split(" ")
        (months(key(0).toInt - 1), rateTypes(key(1).toInt - 1), x._2._1, x._2._2)
      })
      .sortBy(x => x._1 + x._2)
    
    import spark.implicits._

    rdd.toDF
      .write
      .option("sep",";")
      .mode(SaveMode.Overwrite)
      .csv(folderOut + "MonthlyTotalsPerRate")
  }

  def time[R](block: => R, timeLogs: ListBuffer[String], analysisName: String): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    timeLogs += (analysisName + " - Elapsed time: " + (t1 - t0).toFloat/1000 + " seconds")
    result
  }

}

