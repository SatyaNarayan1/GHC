  import scala.util.matching.Regex
  case class log_line (ip_address:String,client:String,user_id:String,date_time:String,method:String,endpoint:String,protocol:String,response_code:Int,content_size:Long)
  def parseFromLine(line:String):log_line = {
    val pattern = new Regex("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)")
    val lines:List[log_line] = (pattern findAllIn line).map
    {_ match { case pattern(  ip_address,client,user_id,date_time,method,endpoint,protocol,response_code,content_size)=>log_line(ip_address,client,user_id,date_time,method,endpoint,protocol,response_code.toInt,content_size.toLong)}
    }.toList
    lines(0)
  }

val logRdd=sc.textFile("/Users/psatya/git/GHC/dataset/access_log")

val logDF=logRdd.map(line=>parseFromLine(line)).toDF

logDF.show
logDf.registerTempTable("logs");
sqlContext.sqlContext().cacheTable("logs");
//Content size analysis 
sqlContext.sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs").show
//Response code count
sqlContext.sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 1000").show
//IP Address access more than 10 times
sqlContext.sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 ").show

//top Endpoints 
sqlContext.sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10").show
