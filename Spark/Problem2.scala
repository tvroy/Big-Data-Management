import scala.collection.Map
//import org.apache.spark.sql.SparkSession

object Problem2 extends Serializable {
    def getGridCellID(point: String):Int={
        val p: Array[String] = point.split(",")
        val a = p(0).toInt
        val b = p(1).toInt
        if (a == 10000 && b == 10000) {
            val GridCellID = 250000
            GridCellID
        } else if (a == 10000) {        
            val GridCellID = ((a/20)) + (500*(b/20))
            GridCellID 
        } else if (b == 10000) {
            val GridCellID = ((a/20)+1) + (499*(b/20))
            GridCellID 
        } else {
            val GridCellID = ((a/20)+1) + (500*(b/20))
            GridCellID 
        }
    }

    def getNeighborCellIDs(gridID: Int):List[Int]={
        // if grid is a corner grid
        if (gridID == 1) {
            val neighborList = List(2,501,502)
            neighborList
        }
        else if (gridID == 500) {
            val neighborList = List(499,999,1000)
            neighborList
        }
        else if (gridID == 249501) {
            val neighborList = List(249001,249002,249502)
            neighborList
        }
        else if (gridID == 250000) {
            val neighborList = List(249499, 249500, 249999)
            neighborList
        }
    
        // if grid lies along one of the sides
        else if (gridID > 1 & gridID < 500) {
            val neighborList = List(gridID-1, gridID+1, gridID+500, gridID+500-1, gridID+500+1)
            neighborList
        }
        else if (gridID > 249501 & gridID < 250000) {
            val neighborList = List(gridID-1, gridID+1, gridID-500, gridID-500-1, gridID-500+1)
            neighborList
        }
        else if (gridID % 500 == 1) {
            val neighborList = List(gridID+500, gridID+500+1, gridID+1, gridID-500, gridID-500+1)
            neighborList
        }
        else if (gridID % 500 == 0) {
            val neighborList = List(gridID+500, gridID+500-1, gridID-1, gridID-500, gridID-500-1)
            neighborList
        }
    
        // all other grids
        else {
            val neighborList = List(gridID-500-1, gridID-500, gridID-500+1, gridID-1, gridID+1, gridID+500-1, gridID+500, gridID+500+1)
            neighborList
        }
    }

    def getRDI(grID: Int, nList: List[Int], hash: Map[Int, Int]):Float={
        var ySum = 0
        val xCount = hash.getOrElse(grID,0)

        for (i <- 0 until nList.length) {
            ySum += hash.getOrElse(nList(i),0)
        }
        
        if ((ySum/nList.length) != 0) {
            val RDI: Float = xCount / (ySum/nList.length)
            RDI
        //if all the neighboring cells are empty, return 0 for the RDI
        } else {
            val RDI: Float = 0
            RDI
        }
    }

    def getNeighborsRDI(nList: List[Int], hash: Map[Int, Float]):String={
        var neighborsRDIValue: String = ""

        for (i <- 0 until nList.length) {
            if (i == nList.length-1) {
                neighborsRDIValue += nList(i).toString + ":" + (hash.getOrElse(nList(i),0)).toString
            }
            else {
                neighborsRDIValue += nList(i).toString + ":" + (hash.getOrElse(nList(i),0)).toString + ","
            }
        }
        neighborsRDIValue
    }

    def main(args: Array[String]):Unit={
       
        /* I didn't end up needing this because spark-shell has a built in spark session and spark context

        val spark = SparkSession
            .builder()
            .appName("Problem2")
            .getOrCreate()
        
        val sc = spark.sparkContext
        */

        val points = sc.textFile(args(0))

        val grids = points.map(p => (getGridCellID(p)))

        val gridsAndCounts = grids.map(g => (g,1)).reduceByKey(_+_)

        //to lookup point counts in each grid
        val gridsAndCountsHashMap = gridsAndCounts.collectAsMap()

        val gridsAndNeighbors = grids.map(gr => (gr,getNeighborCellIDs(gr)))

        val gridsAndRDI = gridsAndNeighbors.map(gn => (gn._1, getRDI(gn._1, gn._2, gridsAndCountsHashMap)))

        val top50 = gridsAndRDI.sortBy(_._2, false).take(50)

        top50.foreach(println)

        //to lookup RDI values for a given grid
        val gridsAndRDIHashMap = gridsAndRDI.collectAsMap()
        
        val top50gridsAndNeighborsRDI = top50.map(t => (t._1, getNeighborsRDI(getNeighborCellIDs(t._1), gridsAndRDIHashMap)))

        top50gridsAndNeighborsRDI.foreach(println)
    }
}