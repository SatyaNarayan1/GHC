val rdd = sc.textFile("/Users/psatya/homeground/spark-1.5.0-bin-hadoop2.6/README.md")
rdd.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_).foreach(println)
