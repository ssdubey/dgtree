package dgtree
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.slf4j.{Logger, LoggerFactory}
import java.util.Calendar

/**
  * This obj is the start of the project. Here we will input the graph and start indexing, then trigger query processing.
  */
object Index {

  val logger:Logger = LoggerFactory.getLogger(getClass)
  var sc:SparkContext = null
  val APPNAME = "Distributed DGTree Indexing"
  val graphDB = collection.mutable.Map[Long, Graph[String, String]] ()         //TODO:try changing Any into graph type
  //val queryGraph = Graph[String, String]

  def main(args: Array[String]) {
        println("starting code at " + Calendar.getInstance().getTime)

    val conf = new SparkConf().setAppName(APPNAME)
    conf.setMaster("local")                     //TODO:this may have to change to run on multiple nodes or from terminal
    conf.set("spark.scheduler.mode", "FAIR")    //TODO:round robin scheduling of the jobs on the same thread
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[DGTreeConstruction], classOf[QueryProcessor]))                                            //TODO: register kryo classes
   // conf.set("spark.executor.instances", "1")
   // conf.set("spark.executor.cores", "1");

    sc   = new SparkContext(conf)
    logger.info("Spark Context created successfully")

    //reading text file to create graph object

    val inputStringConfig = new Configuration()
    inputStringConfig.set("textinputformat.record.delimiter", "#")

    println("started reading graphs at "+ Calendar.getInstance().getTime)
    //var graphInpStrRDD = sc.newAPIHadoopFile("input/datasets/dataset2.txt",      //set commandline arg to specify path
    var graphInpStrRDD = sc.newAPIHadoopFile("/home/shashank/myFolder/dc/superGraphSearch/parallel/sparked-dgtree/sparked-dgtree/datasets/10dataset.txt",
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      inputStringConfig).map(_._2.toString)

    val dataGraphInpStrArray = graphInpStrRDD.collect.drop(1)   //deleting the first element as its empty, may be due to the way we are splitting
    //println("count= "+dataGraphInpStrArray.size)              //TODO: we can save 2 costly steps by correcting this

    graphInpStrRDD = sc.parallelize(dataGraphInpStrArray)   //TODO:it is creating parallel collection rdd. check if its fine or map partition rdd is better

    //segeregating graphid, vertex list and edge list
    graphInpStrRDD.collect.map(graphDetails => {
      val graphDetailList = graphDetails.split("\n")

      val id = graphDetailList(0)

      //create vertex RDD
      val vertexCount = graphDetailList(1).toInt
      val vertexList = graphDetailList.slice(2, 2+vertexCount)
      val vertexListWithReverseIndex = vertexList.zipWithIndex   //format is (A,0)
      val vertexListWithIndex = vertexListWithReverseIndex.map{case (vLabel:String, index:Int) => (index.toLong, vLabel)}
      val vertexListRDD = sc.parallelize(vertexListWithIndex)    //Pattern: vertexListRDD: RDD[(Long, String)]

      //create edge RDD
      val edgeList = graphDetailList.slice(3+vertexCount, graphDetailList.length)
      val edgeObject = edgeList.map(edge => edge.split(" "))
        .map(edgeDetail => Edge(edgeDetail(0).toLong, edgeDetail(1).toLong, edgeDetail(2)))
      val edgeRDD = sc.parallelize(edgeObject)                  //Pattern: edgeRDD: RDD[Edge[String]]

      //create graph object
      val graph = Graph(vertexListRDD, edgeRDD)                 //by default it is a directed graph
      //appending graph in a list(map) of datagraphs with its id as index
      //TODO:check if appending in empty rdd is possible, save 1 step of converting it to rdd
      graphDB(id.toLong) = graph
     //*** println("no. of graphs in db= "+graphDB.size)
      /*graphDB.foreach(graph => {
        println(graph._1)
        graph._2.vertices.foreach(println)
        graph._2.triplets.foreach(println)
      })*/
    })
    //start dgTree Construction
    val worker: DGTreeConstruction = new DGTreeConstruction(graphDB)
    val dgTreeRoot = worker.constructDGTree()

    println("dgtree construction completed at "+ Calendar.getInstance().getTime)
    println("dgtree construction completed")

    println("no. of children at root: "+ dgTreeRoot.children.size)

    //*** showChildren(dgTreeRoot)

    println("query graph processing started at "+ Calendar.getInstance().getTime)
    //var queryGraphInpStrRDD = sc.newAPIHadoopFile("input/query/qtest2.txt",      //set commandline arg to specify path
    var queryGraphInpStrRDD = sc.newAPIHadoopFile("/home/shashank/myFolder/dc/superGraphSearch/parallel/sparked-dgtree/sparked-dgtree/queries/query.txt",      //set commandline arg to specify path
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      inputStringConfig).map(_._2.toString)

    val queryGraphInpStrArray = queryGraphInpStrRDD.collect.drop(1)

    queryGraphInpStrRDD = sc.parallelize(queryGraphInpStrArray)
    queryGraphInpStrRDD.collect.map(graphDetails => {
      val graphDetailList = graphDetails.split("\n")

      val id = graphDetailList(0)

      //create vertex RDD
      val vertexCount = graphDetailList(1).toInt
      val vertexList = graphDetailList.slice(2, 2+vertexCount)
      val vertexListWithReverseIndex = vertexList.zipWithIndex   //format is (A,0)
      val vertexListWithIndex = vertexListWithReverseIndex.map{case (vLabel:String, index:Int) => (index.toLong, vLabel)}
      val vertexListRDD = sc.parallelize(vertexListWithIndex)    //Pattern: vertexListRDD: RDD[(Long, String)]

      //create edge RDD
      val edgeList = graphDetailList.slice(3+vertexCount, graphDetailList.length)
      val edgeObject = edgeList.map(edge => edge.split(" "))
        .map(edgeDetail => Edge(edgeDetail(0).toLong, edgeDetail(1).toLong, edgeDetail(2)))
      val edgeRDD = sc.parallelize(edgeObject)                  //Pattern: edgeRDD: RDD[Edge[String]]

      //create graph object
      val queryGraph = Graph(vertexListRDD, edgeRDD)                 //by default it is a directed graph
      //appending graph in a list(map) of datagraphs with its id as index
      //TODO:check if appending in empty rdd is possible, save 1 step of converting it to rdd

      /*val queryGraph = collection.mutable.Map[Long, Graph[String, String]] ()
      queryGraph(id.toLong) = graph
      println("no. of query graphs= "+queryGraph.size)
      queryGraph(id.toLong).triplets.foreach(println)*/

      val queryProcessor: QueryProcessor = new QueryProcessor           //TODO: this should be directly in main
      val resultantSubgraphs = queryProcessor.superGraphSearch(queryGraph, dgTreeRoot)

println("resultantSubgraphs = " + resultantSubgraphs)
      println("program ends at "+ Calendar.getInstance().getTime)
    })

  }

  var level = 0
  def showChildren(node: DGTreeNode):DGTreeNode={

    //while(node.children.nonEmpty){
    println(s"level = $level"+" children: "+node.children.size)
    node.children.foreach(x => x.growEdge.triplets.foreach(println))
    level = level + 1

    node.children.foreach(x=>showChildren(x))

    //}
    node
  }
}
