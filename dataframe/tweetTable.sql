//Auto detects the schema
val tweetDF=sqlContext.read.json("/Users/psatya/GHC/dataset/part-00001.gz")

//Dataframe DSL
tweetDF.filter("user_location.city is not null and user_location.city!=\"\"").selectExpr("user_location.city","user_name.first").show

//Dataframe SQL 
tweetDF.registerTempTable("tweetTable")

//distinct city 
sqlContext.sql("select distinct user_location.city from tweetTable").count

//JOIN sql

