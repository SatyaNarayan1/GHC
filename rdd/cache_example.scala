val rdd = sc.textFile("/Users/psatya/GHC/dataset/part-000[0-1][0-9].gz")
rdd.cache
rdd.count 
rdd.filter(_.contains("IBM")).count
