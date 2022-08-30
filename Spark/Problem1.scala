case class T(TransID: String, CustID: String, TransTotal: Float, TransNumItems: Int, TransDesc: String)

val transactions = sc.textFile("hdfs://localhost:9000/user/project3/trans_file.txt").
			map(_.split(",")).
			map(t => T(t(0), t(1), t(2).toFloat, t(3).toInt, t(4))).toDF()

transactions.createOrReplaceTempView("transactions")

val T1 = spark.sql("""Select * from transactions
                where TransTotal > 200""")

T1.createOrReplaceTempView("T1")

val T2 = spark.sql("""Select TransNumItems, sum(TransTotal), avg(TransTotal), min(TransTotal), max(TransTotal)
                from T1
                group by TransNumItems""")

T2.createOrReplaceTempView("T2")
T2.show()

val T3 = spark.sql("""Select CustID, count(distinct TransID) as TransCount from T1
                group by CustID""")

T3.createOrReplaceTempView("T3")

val T4 = spark.sql("""Select * from transactions
                where TransTotal < 600""")

T4.createOrReplaceTempView("T4")

val T5 = spark.sql("""Select CustID, count(distinct TransID) as TransCount from T4
                group by CustID""")

T5.createOrReplaceTempView("T5")

val T6 = spark.sql("""Select T5.CustID from T5
                join T3
                on T5.CustID = T3.CustID
                where (T5.TransCount * 5) < T3.TransCount""")

T6.createOrReplaceTempView("T6")
T6.show()