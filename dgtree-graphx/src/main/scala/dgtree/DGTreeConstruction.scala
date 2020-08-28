package dgtree

import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Handles complete construction task of dgTree
  * I am broadcasting some variable in the code. Another method could be to get the output upto that level back and join (with objects which i am broadcasting)
  *
  * @param graphDB Collection of all the datagraphs keyed with index no.
  */
class DGTreeConstruction(graphDB: mutable.Map[Long, Graph[String, String]]) {

  object Ord extends Ordering[DGTreeNode] { // not implicit
    def compare(a:DGTreeNode, b:DGTreeNode) = a.score compare b.score
  }

  val logger:Logger = LoggerFactory.getLogger(getClass)

  /**\
    *
    * This method will check if extendEdgeGraph is a subgraph of featureGraph
    * @param extendEdgeGraph
    * @param featureGraph
    * @return
    */
  def subGraphOf(extendEdgeGraph:Graph[String,String], featureGraph:Graph[String,String]):Boolean= {

    val extendEdgeTriplet = extendEdgeGraph.triplets.take(1)(0) //there has to be only one edge in this graph

    val featureGraphTripletArray = featureGraph.triplets.collect()

    val extendEdgeIndexInFeatureGraph = featureGraphTripletArray.indexOf(extendEdgeTriplet) //indexof only checks the vertexIds

    if (extendEdgeIndexInFeatureGraph != -1) {

      val extendEdgeInFGraph = featureGraphTripletArray(extendEdgeIndexInFeatureGraph) //fetching the selected edge from featre graph

      if ((extendEdgeInFGraph.dstAttr == extendEdgeTriplet.dstAttr) && (extendEdgeInFGraph.srcAttr == extendEdgeTriplet.srcAttr)) {

        return true

      } else {

        println("attribute didnt match, not a subgraph")
        return false

      }

    } else {

      println("not sub graph")
      return false

    }
  }

  /**
    *TODO: check if this can be further optimized
    * @param extendGraph
    * @param edgeAndNodeMap
    * @return
    */
  def findExtendGraphInHeap(extendGraph: Graph[String, String], edgeAndNodeMap: mutable.Map[Graph[String, String], DGTreeNode]): _root_.dgtree.DGTreeNode = {

    val graphsInHeap = edgeAndNodeMap.keySet
    val dgNodeInHeap = graphsInHeap.map(graph => {
      val isSubGraph = subGraphOf(extendGraph, graph)
      //TODO: do something to stop it from searching graphs once result is found
      if(isSubGraph == true){
        edgeAndNodeMap(graph)
      }else{
        null
      }

    })

    val dgNode = dgNodeInHeap.filter(node => node!=null)

    if(dgNode.size == 0){
      println("node not in heap")
      null
    }else if(dgNode.size == 1){
      println("node found in heap")
      dgNode.head
    }else{
      println("there is some issue while extracting from heap")
      null
    }
  }

  /**
    *
    * @param node
    * @param heap
    * @param phase
    * @return
    */
  def phaseCandidateFeature(node: DGTreeNode, heap: mutable.PriorityQueue[DGTreeNode], phase:Int): mutable.Map[Graph[String, String], DGTreeNode] = {
    println("entering candidate feature phase")
    var edgeAndNodeMap = mutable.Map[Graph[String, String], DGTreeNode]()                    //this map will contain the heap data

    //read each graph whose feature is present in this node(node.sStar) and traverse through each of its matches
    var supportedDatagraphs: mutable.Map[Long, Graph[String, String]] = new mutable.HashMap[Long, Graph[String, String]]()

    if(phase == 1){

      supportedDatagraphs = node.sStar
      println("supportedDatagraphs in s* in phase1 : ")

    }
    else{

      supportedDatagraphs = node.s
      println("supportedDatagraphs in s in phase2 : ")

    }

    supportedDatagraphs.map(graph => {
      logger.info("taking each graph in s* of node. Format: (Long, Graph[String, String])")
      println("considered graph: "+ graph._1)
      val matches = node.matches.get(graph._1)                                      //matches which belong to selected graph's id; format:  Option[RDD[ListBuffer[Long]]]

      val nbrsIdAndNameRDD = graph._2.collectNeighbors(EdgeDirection.Either).collect()    //  broadcast it
      // collecting all the neighbors(in array) corresponding to each vertex in 2d array format
      val neighborBrdcst = Index.sc.broadcast(nbrsIdAndNameRDD)


      //*** println("checkinig matches for considered graph")
      //*** matches.foreach(item=>{
      //***   item.collect.foreach(arrbuf=>println(arrbuf))
      //*** })
      matches.map(mtchArrBufRDD => {                                                      //mtchRDD format:  RDD[ListBuffer[Long]]
        println("fetching the next match")
        /* mtchRDD is the collection of listbuffers each of which contains the vertices belonging to a feature.
         * (TODO: as of 1/12 i understand that each node can have only one feature, so apart from root node every
         * other node will have only one list buffer with multiple vertices, and root will have multiple listbuffers
         * with one vertex in each as it considers each vertex for potential feature initially)*/


        mtchArrBufRDD.collect.map(mtchArrBuf => {
          println(mtchArrBuf)
          mtchArrBuf.map(matchLstVerticesNameAndIdIn_g => { //matchLstVerticesNameAndIdIn_g format:  ArrayBuffer[(String, Long)]. collection of vertices belonging to one feature
            println("matches: " + matchLstVerticesNameAndIdIn_g.toList)

            matchLstVerticesNameAndIdIn_g.map(vertexInFeature => { //vertexInFeature is vertexId of an individual vertex which can be in a feature
              println("considered vertex in feature: " + vertexInFeature)
              var ui: Int = matchLstVerticesNameAndIdIn_g.indexOf(vertexInFeature) //ui is the index of vertexInFeature in matchLstVerticesNameAndIdIn_g
              var uiName = vertexInFeature._1 //vertex name which is already in feature(for labelled implementation)
              println("ui and name: " + ui + "   " + uiName)

              val filteredNeighbors = (neighborBrdcst.value).filter(vertexIdAndNbr => vertexIdAndNbr._1 == vertexInFeature._2.toInt)


              /*val filteredNeighbors = nbrsIdAndNameRDD
                .filter(vertexIdAndNbr => vertexIdAndNbr._1 == vertexInFeature._2.toInt)*/


              //***  println("filteredNeighbors: ")
              //*** filteredNeighbors.foreach(nei => {
              //***   println("nbr of vertex: " + nei._1)
              //***   println(nei._2.toList)
              //*** })

              filteredNeighbors.map(vertexIdAndNbr => { //TODO:check if map can be removed with a (0) here //here no. of rows in the array(vertexIdAndNbr) is equal to no. of neighbors. fileteredNeighbors is just a cover over array
                println("considered vertex: " + vertexIdAndNbr._1)
                vertexIdAndNbr._2.map(nbrIdAndName => { //TODO: vertexIdandnbr seems to be one value always, confirm and remove foreach then

                  println("vertex" + vertexIdAndNbr._1 + ", its neighbor: " + nbrIdAndName)
                  var edgeType: String = ""
                  var uj: Int = 0
                  var ujName: String = ""
                  //TODO: below code may always return no becuase of type difference priority 1(tried to correct, do the dry run)
                  if (matchLstVerticesNameAndIdIn_g.contains(nbrIdAndName.swap)) { //if this neighbor node is already present in the feature graph
                    //TODO:correct this id and name thing to avoid swapping
                    println("neighbor node already present in feature graph")
                    uj = matchLstVerticesNameAndIdIn_g.indexOf(nbrIdAndName.swap) //uj is the index of neighbor node in the feature graph
                    ujName = matchLstVerticesNameAndIdIn_g(uj)._1 //vertex name for the potential vertex which can be taken in the feature next
                    println("uj and ujname: " + uj + "  " + ujName)
                    if (phase == 1) {
                      edgeType = EdgeType.CLOSE.toString

                      println("edgeType = " + edgeType)
                    }


                  } else {
                    println("neighbor node NOT present in feature graph")
                    uj = matchLstVerticesNameAndIdIn_g.size //vertexId starts with 0
                    ujName = nbrIdAndName._2

                    if (phase == 1) {
                      edgeType = EdgeType.OPEN.toString

                      println("edgeType = " + edgeType)
                    }


                  }

                  /*preparing growEdge*/
                  val extendVertRDD = Index.sc.parallelize(Array((ui.toLong, uiName), (uj.toLong, ujName)))
                  val extendEdgeRDD = Index.sc.parallelize(Array(Edge(ui.toLong, uj.toLong, "1"))) //TODO: considering edges with valence 1 only. change it for variable
                  val extendGraph = Graph(extendVertRDD, extendEdgeRDD)

                  println("growEdge vertices: ")
                  //***  extendGraph.vertices.foreach(println)

                  println("growEdge edges: ")
                  //***  extendGraph.edges.foreach(println)

                  /*val featureGraphEdgesRDD = node.featureGraph.edges
                  println("feature graph edges of current node: ")
                  featureGraphEdgesRDD.collect.foreach(println)
                  node.featureGraph.vertices.foreach(println)*/

                  //TODO: if i can make heap of the following format, i can save one map transformation step
                  //edgeAndNodeArray is a temporary heap, so first time fill it with heap values and keep updating with new values, then return it back to tree grow and update the real heap
                  if (edgeAndNodeMap.isEmpty) {
                    val edgeAndNodeArray = heap.map(candidateNode => {

                      val growEdgeGraph = candidateNode.growEdge//.edges.take(1)(0)

                      (growEdgeGraph, candidateNode)

                    })

                    /*creating a local heap and updating the values into it. later return it back to cadidatefeature and update the original heap*/
                    edgeAndNodeMap = collection.mutable.Map((edgeAndNodeArray.toMap).toSeq: _*) //toMap converts array to immutable map, i need mutable
                  }

                  var gPlus = new DGTreeNode
                  val extendEdge = extendEdgeRDD.collect().take(1)(0) //since there is only one element in the rdd, hence index 0

                  //*** node.featureGraph.triplets.foreach(println)
                  if ((uj > ui) && (!subGraphOf(extendGraph, node.featureGraph))){//TODO:change this to notSubGraph and save one operation...(featureGraphEdgesRDD.collect().contains(extendEdge)))) { //edges contains only vertexId and not vertex Names, so this becomes unlabelled implementation, change this to extend graph to make the labelled implementation
                    println(uj + ">" + ui + ",extend edge not in feature graph")
                    //gPlus = edgeAndNodeMap.getOrElse(extendGraph, null) //looking if the similar edge is already created TODO:replace null with whatever is preffered in scala
                    gPlus = findExtendGraphInHeap(extendGraph, edgeAndNodeMap)

                    if (phase == 1) {

                      if (gPlus == null) {
                        println("gplus is null in phase 1")
                        gPlus = new DGTreeNode

                        gPlus.growEdge = extendGraph //this graph contains the single edge and its vertices

                        gPlus.sStar(graph._1) = graph._2

                        gPlus.score = 0

                        gPlus.edgeType = edgeType

                        // val newGrowEdge = gPlus.growEdge.edges.collect().take(1)(0)
                        edgeAndNodeMap += ((extendGraph, gPlus)) //entering edge as key so that searching is fast, also its temporary, for this round of loop only
                        println("edgeAndNodeMap updated with new node")
                        //println("printing size2: " + edgeAndNodeMap.size)

                      } else {
                        println("gplus is not null in phase 1")
                        gPlus.sStar(graph._1) = graph._2

                      }
                    } else { //phase2
                      if (gPlus != null) {
                        println("gplus is not null in phase 2")
                        gPlus.s(graph._1) = graph._2

                        if (gPlus.edgeType == EdgeType.OPEN.toString) {
                          //***  println("proceeding for open edge case")
                          //TODO: check it with dryrun and optimize it (too many variables)
                          //var newMatches = matchLstVerticesNameAndIdIn_g
                          //***  println("1")

                          val newMatchesArrayBuffer = matchLstVerticesNameAndIdIn_g.init :+ matchLstVerticesNameAndIdIn_g.last     //=f  //matchLstVerticesNameAndIdIn_g is the existing match
                          newMatchesArrayBuffer += ((nbrIdAndName._2, nbrIdAndName._1)) //{[f,v]}
                          //***   println("2")

                          //     if (gPlus.matches.isEmpty) {    //it is a first match for the given node gplus
                          if(!gPlus.matches.keySet.contains(graph._1)){
                            //***    println("3")
                            val newMatchRDD = Index.sc.parallelize(Array(ArrayBuffer(newMatchesArrayBuffer)))
                            gPlus.matches(graph._1) = newMatchRDD

                          } else {      //gplus already have matches. therefore, the new match is another entity and should not be mixed with prev one
                            println("4")
                            /*try{
                              val currentMatchIn_gPlus = gPlus.matches(graph._1).collect   //=f  // Array[ArrayBuffer[ArrayBuffer[(String, Long)]]]
                            }catch{
                              case e: Exception => println("exception occured")
                            }*/
                            val currentMatchIn_gPlus = gPlus.matches(graph._1).collect   //=f  // Array[ArrayBuffer[ArrayBuffer[(String, Long)]]]
                            //remove the array and put newMatchesArrayBuffer inside outer arraybuffer
                            //currentMatchIn_gPlus is an array, but it will always have just 1 element
                            val newMatchesFor_gPlusArrayBuffer = currentMatchIn_gPlus(0):+newMatchesArrayBuffer
                            val newMatchRDD = Index.sc.parallelize(Array(newMatchesFor_gPlusArrayBuffer))
                            gPlus.matches(graph._1) = newMatchRDD

                          }
                          println("5")
                        } else {
                          println("proceeding for close edge case")

                          val extendEdgeTriplet = extendGraph.triplets.take(1)(0) //there has to be only one edge in this graph
                          println("extendEdgeTriplet"+extendEdgeTriplet)
                          val featureGraphTripletArray = node.featureGraph.triplets.collect()
                          println("featureGraphTripletArray")
                          //***  featureGraphTripletArray.foreach(println)

                          //if (gPlus.matches(graph._1).isEmpty()) {    //it is a first match for the given node gplus
                          if(!gPlus.matches.keySet.contains(graph._1)){
                            //***  println("3")
                            val newMatchRDD = Index.sc.parallelize(Array(ArrayBuffer(matchLstVerticesNameAndIdIn_g)))   //f //matchLstVerticesNameAndIdIn_g is the existing match
                            gPlus.matches(graph._1) = newMatchRDD

                          } else { //gplus already have matches. therefore, the new match is another entity and should not be mixed with prev one

                            val currentMatchIn_gPlus = gPlus.matches(graph._1).collect //this is existing match in gplus
                            val newMatchesFor_gPlusArrayBuffer = currentMatchIn_gPlus(0) :+ matchLstVerticesNameAndIdIn_g //appending new matches in existing matches
                            val newMatchRDD = Index.sc.parallelize(Array(newMatchesFor_gPlusArrayBuffer))
                            gPlus.matches(graph._1) = newMatchRDD
                          }
                        }
                      }
                      println("no else part if gplus is null in phase 2")
                    }
                  }
                })
              })
            })
          })
        })
      })
    })
    edgeAndNodeMap
  }


  def calculateScore(node: DGTreeNode) = {  //TODO:check if its correct and modify variable names  priority 2(check with dry run)

    println("calculating score for node with growEdge: ")
    //***node.growEdge.edges.foreach(println)

    val graphsInS = node.s.keySet
    //***println("no. of graphs in s= "+ graphsInS.size)

    val filteredMatches = node.matches.filter(m => graphsInS.contains(m._1))
    //*** println("filteredMatches size = "+ filteredMatches.size)

    val matchesCount = filteredMatches.values.toList
      .flatten(rdd=>rdd.collect)
      .flatten(ab=>ab)
      .flatten(e=>e)
      .size
    /*val lst = rddsList.flatten(rdd=>rdd.collect)
    val ab = lst.flatten(ab=>ab)
    val ele = ab.flatten(e=>e)
    val s = ele.size*/




    // val matchesCount = filteredMatches.values.toList.size
    println("matchesCount = "+ matchesCount)
    //println("mc= "+matchesCount)
    //TODO: this is scam
    if(matchesCount != 0){
      node.score = (node.sStar.size * node.s.size).toLong/matchesCount
    }else{
      node.score = 0
    }


    //println("score= "+ node.score)

  }

  val MAX_SCORE = 1000    //constant value to be added in the score if edge type is close

  def candidateFeature(node: DGTreeNode): mutable.PriorityQueue[DGTreeNode] = {
    println("entering candidateFeature")
    //creating dummy node to initialize heap
    val dummyVertRDD = Index.sc.parallelize(Array((-1.toLong, "dummyV")))
    val dummyEdgeRDD = Index.sc.parallelize(Array(Edge(-1.toLong, -1.toLong, "dummyE")))
    val dummyGraph = Graph(dummyVertRDD, dummyEdgeRDD)

    val dummyNode = new DGTreeNode
    dummyNode.growEdge = dummyGraph
    println("created dummy node and and inserted dummy graph in growedge")

    val heap = new mutable.PriorityQueue[DGTreeNode]()(Ord)
    heap += dummyNode                 //initializing heap to avoid error in code due to empty heap
    println("putting dummynode in heap, heapsize=  "+ heap.size)

    val heapMap = phaseCandidateFeature(node, heap, 1)
    //*** println("phase 1 completed, printing heap: ")
    //*** heapMap.foreach(println)


    var item = heap.dequeue()     //TODO: how to be sure that it is removing dummy edge only, though it is actually removing it only

    println("dequeued item")
    //***item.growEdge.edges.foreach(println)
    //***println("removing dummy node from heap=  "+ heap.size)

    val heapvals = heapMap.values.toList

    heapvals.foreach(treeNode => {
      if(treeNode!=dummyNode){        //TODO:heapvals already contains dummy node, check if this can be done in a better way
        heap += treeNode
      }

    })

    /*item = heap.dequeue()

    println("dequeued item")
    item.growEdge.edges.foreach(println)*/

    println("updated original heap, contents: ")
    heap.foreach(item=>item.growEdge.vertices.foreach(println)) //heap.foreach(item=>item.growEdge.edges.foreach(println))

    phaseCandidateFeature(node, heap, 2)
    println("completed phase 2")

    //var l = 0
    //calculate score for nodes in heap
    heap.map( node => {

      calculateScore(node)

      //l = l+1

      //TODO: push score updated node into heap (done, check it)
      if(node.edgeType == EdgeType.CLOSE.toString){

        node.score = node.score + MAX_SCORE

      }

    })

    //updating heap according to new scores
    val nodes = heap.dequeueAll
    nodes.foreach(node => heap.enqueue(node))

    return heap
  }

  def bestFeature(heap: mutable.PriorityQueue[DGTreeNode], candidates: mutable.Map[Long, Graph[String, String]]): DGTreeNode = {
    println("finding the best feature in the heap")

    println("no. of elements in heap before dequeue= "+heap.size)

    try {   //TODO: putting this try catch to handle the condition when heap is null but candidates are not. I think this is the time when one recursion should get over

      var newNode = heap.dequeue() //TODO:check if it returns null(dry run)
      //***  println("no. of elements in heap after dequeue= " + heap.size)
      var subset = newNode.sStar.keySet.toList.forall(candidates.keySet.toList.contains)

      while (!subset) {

        val uncoveredGraphs = newNode.sStar.keySet.intersect(candidates.keySet)
        newNode.sStar = newNode.sStar.filter(graphsKV => uncoveredGraphs.contains(graphsKV._1))
        if (newNode.sStar.nonEmpty) {
          calculateScore(newNode)
          heap.enqueue(newNode)
        }
        //***   println("heap size before dequeue: " + heap.size)
        if (heap.size > 0) { //TODO: putting this condition because of anamoly coming during debug and run mode
          newNode = heap.dequeue()
        }
        subset = newNode.sStar.keySet.toList.forall(candidates.keySet.toList.contains)
      }

      return newNode

    }catch{

      case e: NoSuchElementException => {
        println("heap is empty but candidates have value, so returnig to previous recursive frame")
        return null

      }
    }
  }


  def createFGraphEdges(featureGraphEdges: EdgeRDD[String], growEdge: EdgeRDD[String]): RDD[Edge[String]] = {
    val fGraphCollection = featureGraphEdges.collect()
    val growEdgeCollection = growEdge.collect()
    val newEdges = (fGraphCollection.toList ++ growEdgeCollection.toList).distinct

    Index.sc.parallelize(newEdges)
  }

  def createFGraphVertices(featureGraphVertices: VertexRDD[String], growEdgeVertices: VertexRDD[String]): RDD[(graphx.VertexId, String)] = {
    val fGraphCollection = featureGraphVertices.collect()
    val growEdgeCollection = growEdgeVertices.collect()
    val newVertices = (fGraphCollection.toList ++ growEdgeCollection.toList).distinct

    Index.sc.parallelize(newVertices)
  }

  def treeGrow(node: DGTreeNode): DGTreeNode={

    try {
      println("entering treeGrow")
      //val candidateHeap: mutable.PriorityQueue[DGTreeNode] = mutable.PriorityQueue.empty[DGTreeNode]
      //var heap = new java.util.concurrent.PriorityBlockingQueue[DGTreeNode]()(Ord)
      var heap = new mutable.PriorityQueue[DGTreeNode]()(Ord)
      //var heap = DGTreeConstruction.heap

      val heapVal = candidateFeature(node)

      if (heapVal.size > 0) {
        heap = heapVal.init
        heap += heapVal.last
      } else {
        println("heapval is empty") //TODO: may have to remove this condition, confirm by more graphs
      }


      println("returning from candidate feature for node with growEdge: ")
      //***  node.growEdge.edges.foreach(println)
      println("no. of elements in heap in treegrow= " + heap.size)

      var candidates = node.sStar
      println("no. of candidates: " + candidates.size)
      //val item = candidates.nonEmpty

      while (candidates.nonEmpty) {
        println("no. of elements in heap in before sending to bestfeature= " + heap.size)

        var newNode = bestFeature(heap, candidates)

        if (newNode.sStar.size > 1) {
          println("s* size > 1")

          //during the first iteration both grow edge and feature graph are empty
          if (node.growEdge.edges.isEmpty() && node.featureGraph.edges.isEmpty()) {
            println("both growEdge and feature graph are empty")
            //root is the parent, which does not have any grow edge
            val newFGraphEdges = newNode.growEdge.edges

            var newFGraphVertices = newNode.growEdge.vertices

            newNode.featureGraph = Graph(newFGraphVertices, newFGraphEdges)

            //***    newNode.featureGraph.triplets.foreach(println)
            println("printed feature graph triplet")
            //newNode.featureGraph = null//Graph(newFGraphVertices, newFGraphEdges)

          } else if (node.featureGraph.edges.isEmpty()) { //TODO: this may need to be removed, check
            val newFGraphEdges = node.growEdge.edges

            var newFGraphVertices = node.growEdge.vertices

            newNode.featureGraph = Graph(newFGraphVertices, newFGraphEdges)

            //***    newNode.featureGraph.triplets.foreach(println)
            println("printed feature graph triplet")
          } else {
            //both are not empty

            //val newFGraphEdges =
            val newFGraphEdgesRDD = createFGraphEdges(node.featureGraph.edges, newNode.growEdge.edges)
            var newFGraphVerticesRDD = createFGraphVertices(node.featureGraph.vertices, newNode.growEdge.vertices)
            println("fGraphParam created")
            //val newFGraphEdges = node.featureGraph. node.growEdge

            //var newFGraphVertices = node.featureGraph.vertices ++ node.growEdge.vertices

            newNode.featureGraph = Graph(newFGraphVerticesRDD, newFGraphEdgesRDD)

            //***     newNode.featureGraph.triplets.foreach(println)
            println("printed feature graph triplet")
          }

          //     println(node.growEdge.edges.count())
          /* node.featureGraph.edges.foreach(println)
         node.growEdge.edges.foreach(println)*/


          /*println("edges and vertices for the new fgraph:")
        newFGraphEdges.foreach(println)
        newFGraphVertices.foreach(println)*/

          // newNode.featureGraph = Graph(newFGraphVertices, newFGraphEdges)

          treeGrow(newNode)

        } else {
          println("s* size = 1")
          var fgEdgeCount = 0.toLong
          val dgEdgeCount = newNode.sStar.values.head.edges.count()
          try {
            if (node.growEdge.edges.isEmpty() && node.featureGraph.edges.isEmpty()) {
              println("both growEdge and feature graph are empty")
              //root is the parent, which does not have any grow edge
              val newFGraphEdges = newNode.growEdge.edges

              var newFGraphVertices = newNode.growEdge.vertices

              newNode.featureGraph = Graph(newFGraphVertices, newFGraphEdges)

              //fgEdgeCount = newNode.featureGraph.edges.count()

              //***    newNode.featureGraph.triplets.foreach(println)
              println("printed feature graph triplet")
              //newNode.featureGraph = null//Graph(newFGraphVertices, newFGraphEdges)

            }else{
              fgEdgeCount = node.featureGraph.edges.count()
              if (dgEdgeCount > fgEdgeCount) {
                val newFGraphEdgesRDD = createFGraphEdges(node.featureGraph.edges, newNode.growEdge.edges)
                var newFGraphVerticesRDD = createFGraphVertices(node.featureGraph.vertices, newNode.growEdge.vertices)
                newNode.featureGraph = Graph(newFGraphVerticesRDD, newFGraphEdgesRDD)
              }
              }

            //extending feature graph to avoid isomorphism checking and covering each edge in the feature graph node by node

            //val dgVertexCount = newNode.sStar.values.head.vertices.count()


            /*if (dgEdgeCount > fgEdgeCount) {
              val newFGraphEdgesRDD = createFGraphEdges(node.featureGraph.edges, newNode.growEdge.edges)
              var newFGraphVerticesRDD = createFGraphVertices(node.featureGraph.vertices, newNode.growEdge.vertices)
              newNode.featureGraph = Graph(newFGraphVerticesRDD, newFGraphEdgesRDD)
*/
              fgEdgeCount = newNode.featureGraph.edges.count()
              if (dgEdgeCount > fgEdgeCount) {
                treeGrow(newNode)
              } else {
                newNode.featureGraph = newNode.sStar.values.head //since there is only one element
              }
            /*} else { //TODO:remove this one
              newNode.featureGraph = newNode.sStar.values.head //since there is only one element
            }*/
          }catch{
            case e: Exception => {
              println("returning to previous frame in treegrow")
              println(e.getCause)
              println(e.getMessage)
            }
          }
        }
        node.children += newNode //TODO: change the children data type as per the query processor

        //*** println("children for the node with growEdge: ")
        //*** node.growEdge.edges.foreach(println)

        //***  if (node.children(0).growEdge.edges.isEmpty()) {
        //***    println("grow edge is empty")
        //*** } else {
        //***   println("children edge count: ")
        //***   println(node.children(0).growEdge.edges.count())
        //*** }

        //***  node.children.foreach(child => {
        //***    child.growEdge.edges.foreach(println)
        //***  })
        candidates = candidates.filter(x => !newNode.sStar.contains(x._1)) //removing the elements which are in intersection with g+.sstar
      }
    }catch{
      case e: Exception => {
        println("returning to previous frame in treegrow")
        println(e.getCause)
        println(e.getMessage)
      }
    }
    node        //no need to return anything but doing so as scala demands to return something
  }

  /**
    * This method will create a new root node with values of s, s* and matches(possible extensions)
    * Further it will call the treeGrow method which will further extend the dgTree
    */
  def constructDGTree(): DGTreeNode = {

    //create a new root node and initialize it
    val root = new DGTreeNode
    root.s = graphDB
    root.sStar = graphDB

    //putting all vertices of each graph as a match for the root node
    //var matches:Map[Long, List[Long]] = null
    graphDB.foreach(graph => {
      val graphId = graph._1 //key in the map

      //val vertexList: ArrayBuffer[(String, Long)] = new ArrayBuffer[(String, Long)]

      val vList = graph._2.vertices.map(vertex => {
        val vertexList: ArrayBuffer[(String, Long)] = new ArrayBuffer[(String, Long)]
        //vertexList.clear()                      //TODO:vertexList was getting prepared in a wierd way, so had to clear it
        //every time. correctness is not sure yet.
        vertexList.+=((vertex._2, vertex._1.toLong))
        //println("item going in vertexList = "+ vertex._1 +"  " + vertex._2)
        //ArrayBuffer(vertexList)
        ArrayBuffer(vertexList)
      })
      /*println("vlist = ")
            vList.take(1).foreach(println)*/
      /*vList.foreach(item=>{
        println("vlist = "+item)
      })*/
      root.matches(graphId) = vList             //TODO: converted the value part from ListBuffer to RDD[ListBuffer...
      //*** println("matches done for graph id "+graphId)
      //***  vList.foreach(arrbuf=>arrbuf.foreach(println))
      //TODO: check here for any related error
    })
    treeGrow(root)

    root                                        //returning root to the main code for query processing
  }
}