package dgtree

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class QueryProcessor {

  var resultantSubGraphs: mutable.HashSet[Long] = new mutable.HashSet[Long]()

  object Ord extends Ordering[QueryNode] { // not implicit
    def compare(a:QueryNode, b:QueryNode) = a.score compare b.score
  }

  val queryGraphId = 1          //TODO: remove queryGraphId variable in the code

  /**
    *
    * @param heap
    * @param candidates
    * @return
    */
  def bestFeature(heap: mutable.PriorityQueue[QueryNode],
                  candidates: mutable.Map[Long, Graph[String, String]]): QueryNode = {

    try {
      //***println("finding the best feature in the heap")

      //***println("no. of elements in heap before dequeue= " + heap.size)
      var newNode = heap.dequeue() //TODO:check if it returns null(dry run)
      //***println("no. of elements in heap after dequeue= " + heap.size)
      var subset = newNode.sStar.keySet.toList.forall(candidates.keySet.toList.contains)

      while (!subset) {

        val uncoveredGraphs = newNode.sStar.keySet.intersect(candidates.keySet)
        newNode.sStar = newNode.sStar.filter(graphsKV => uncoveredGraphs.contains(graphsKV._1))
        if (newNode.sStar.nonEmpty) {
          calculateScore(newNode)
          heap.enqueue(newNode)
        }

        newNode = heap.dequeue()
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

  def calculateScore(node: QueryNode)={

    val sStarCount = node.sStar.size
    val matchesCount = node.matches(queryGraphId).count()
    if(matchesCount != 0){
      node.score = sStarCount.toDouble / matchesCount
    }else{
      node.score = 0
      //*** println("problem in calculating score")
    }
    //***println(node.score)
  }

  /**
    *
    * @param queryGraphId
    * @param queryGraph
    * @param queryNode
    * @param gPlus
    * @param heap
    * @param candidateDGs
    * @param childrenCount
    * //@param resultantSubGraphs
    */
  def featureExpansion(queryGraphId: Int,
                       queryGraph: Graph[String, String],
                       queryNode: QueryNode,
                       gPlus: DGTreeNode,
                       heap: mutable.PriorityQueue[QueryNode],
                       candidateDGs: mutable.Map[Long, Graph[String, String]],
                       childrenCount: Int): collection.Set[Long] ={
    //resultantSubGraphs: mutable.HashSet[Long]): mutable.HashSet[Long] ={

    val qPlus = new QueryNode
    qPlus.treeNode = gPlus
    var newMatchFound:Boolean = false

    //TODO: change the names of the below variables
    val graphsInS = gPlus.s.keySet
    var candidateGraphs = candidateDGs.keySet
    val commonGraphs = graphsInS.intersect(candidateGraphs)
    qPlus.sStar = gPlus.s.filter(graph => commonGraphs.contains(graph._1))

    val growEdgeGraph = gPlus.growEdge

    val nbrsIdAndNameRDD = queryGraph.collectNeighbors(EdgeDirection.Either).collect()//TODO: instead of calculating it everytime, save it in a global location

    val matches = queryNode.matches.get(queryGraphId)   //TODO:check to remove this get part as there is only one item in the map

    matches.map(mtchArrBufRDD => {
      mtchArrBufRDD.collect.map(mtchArrBuf => {
        mtchArrBuf.map(mtch => { //mtch is f which belongs to q.M(Q)

          //*** println("selecting a match: ")
          //***println(mtch)

          val growEdgeVertices = gPlus.growEdge.vertices.take(2)
          val ui = if (growEdgeVertices(0)._1 < growEdgeVertices(1)._1) growEdgeVertices(0) else growEdgeVertices(1) //considering smaller vertex id will always be present in case of open edge(not sure)
          val uj = if (growEdgeVertices(0)._1 > growEdgeVertices(1)._1) growEdgeVertices(0) else growEdgeVertices(1) //TODO:optimize it

          //***println("ui, uj at growedge = ")
          //***println(s"ui = $ui, uj = $uj")

          //val matchVertices = mtch.map(vertex => vertex._1)
          val matchVerticesMap = mtch.toMap

          //  if(mtch(0)._1 == ui._2){  //the match we are growing further should start with grow edge vertex TODO: need to check with further levels of tree
          if(matchVerticesMap.contains(ui._2)){  //the match we are growing further should start with grow edge vertex TODO: need to check with further levels of tree
            //val ind = matchVerticesMap(ui._2)

            val filteredNeighbors = nbrsIdAndNameRDD
              .filter(vertexIdAndNbr => {
                queryGraph.vertices.lookup(vertexIdAndNbr._1)(0) == ui._2
              })

            filteredNeighbors.map(vertexIdAndNbr => { //TODO:check how many elements are in filteredNeighbors, if only one then change map to (0)
              val ind = vertexIdAndNbr._1

              if(matchVerticesMap.values.toList.contains(ind.toLong)){
                vertexIdAndNbr._2.map(nbr => { //nbr = v

                  //check if neighbor is equivalent to uj
                  if (nbr._2 == uj._2) {

                    if (gPlus.edgeType == EdgeType.OPEN.toString) {

                      if (!mtch.contains(nbr.swap)) { //v!=f      //in algo this and above line are swapped

                        //***println("neighbor is same as uj")
                        if (childrenCount == 1) { //TODO:to be changed to some readable format

                          //***println("this node has further children")
                          val newMatchesArrayBuffer = mtch.init :+ mtch.last //=f  //the existing match
                          newMatchesArrayBuffer += ((nbr._2, nbr._1)) //{[f,v]}

                          //val item = Edge(mtch(0)._2,nbr._1,"1")
                          val item = Edge(ind.toLong,nbr._1,"1")

                          //***println("edge we are looking for is: " + item)

                          //TODO: this if might go off
                          if(queryGraph.edges.collect().contains(Edge(ind.toLong, nbr._1,"1"))||queryGraph.edges.collect().contains(Edge(nbr._1, ind.toLong,"1"))){ //to avoid wrong edges due to similar vertex names
                            if (!qPlus.matches.keySet.contains(queryGraphId)) {

                              val newMatchRDD = Index.sc.parallelize(Array(ArrayBuffer(newMatchesArrayBuffer)))
                              qPlus.matches(queryGraphId) = newMatchRDD

                            } else { //qplus already have matches. therefore, the new match is another entity and should not be mixed with prev one

                              val currentMatchIn_qPlus = qPlus.matches(queryGraphId).collect //=f  // Array[ArrayBuffer[ArrayBuffer[(String, Long)]]]
                              //remove the array and put newMatchesArrayBuffer inside outer arraybuffer
                              //currentMatchIn_qPlus is an array, but it will always have just 1 element
                              val newMatchesFor_qPlusArrayBuffer = currentMatchIn_qPlus(0) :+ newMatchesArrayBuffer
                              val newMatchRDD = Index.sc.parallelize(Array(newMatchesFor_qPlusArrayBuffer))

                              val item = qPlus.matches(queryGraphId).collect()(0)
                              if(!(qPlus.matches(queryGraphId)).collect()(0).contains(newMatchRDD)){    //@line 142 nbr are coming for 2 times, dont know why, check that and remove this condition
                                qPlus.matches(queryGraphId) = newMatchRDD
                              }

                            }
                            //***println("qPlus matches: ")
                            println(qPlus.matches(queryGraphId).collect())
                          }
                        } else {

                          println("this node does not have any more children")
                          //newMatchFound = true
                          //TODO:we are just checking one match and concluding that it is ans graph, which is not right
                          val item = Edge(ind.toLong,nbr._1,"1")

                          if(queryGraph.edges.collect().contains(Edge(ind.toLong,nbr._1,"1"))||queryGraph.edges.collect().contains(Edge(nbr._1,ind.toLong,"1"))) {
                            resultantSubGraphs += gPlus.s.head._1
                            println(resultantSubGraphs)
                          }
                        }
                      }
                    } else {//close edge

                      if (mtch.contains(nbr.swap)) {

                        //val matchesMap = mtch.map(f => (f._2, f._1)).toMap
                        val qGraphIndUi = mtch(ui._1.toInt)
                        val qGraphIndUj = mtch(uj._1.toInt)

                        val item = Edge(qGraphIndUi._2.toLong, qGraphIndUj._2.toLong, "1") //close edge which should be present in the query graph

                        if (queryGraph.edges.collect().contains(Edge(qGraphIndUi._2.toLong, qGraphIndUj._2.toLong, "1")) || queryGraph.edges.collect().contains(Edge(qGraphIndUj._2.toLong, qGraphIndUi._2.toLong, "1"))) { //to avoid wrong edges due to similar vertex names
                          println("close edge condition")


                          if (childrenCount == 1) {
                            if (!qPlus.matches.keySet.contains(queryGraphId)) {
                              val newMatchRDD = Index.sc.parallelize(Array(ArrayBuffer(mtch))) //f //the existing match
                              qPlus.matches(queryGraphId) = newMatchRDD

                            } else { //qplus already have matches. therefore, the new match is another entity and should not be mixed with prev one

                              val currentMatchIn_qPlus = qPlus.matches(queryGraphId).collect //this is existing match in gplus
                              val newMatchesFor_qPlusArrayBuffer = currentMatchIn_qPlus(0) :+ mtch //appending new matches in existing matches
                              val newMatchRDD = Index.sc.parallelize(Array(newMatchesFor_qPlusArrayBuffer))
                              qPlus.matches(queryGraphId) = newMatchRDD
                            }
                          } else {
                            resultantSubGraphs += gPlus.s.head._1
                            println(resultantSubGraphs)
                          }

                        } //else
                      }
                    }
                  }//nbr
                })
              }
            })
          }//mtch==ui
        })
      })
    })

    //if(qPlus.matches(queryGraphId).count() > 0){
    if(qPlus.matches.keySet.contains(queryGraphId)&&(qPlus.matches(queryGraphId).count() > 0)){
      calculateScore(qPlus)
      heap.enqueue(qPlus)
    }else{
      candidateGraphs = candidateGraphs.diff(qPlus.sStar.keySet)
      //candidateDGs = candidateDGs.filter(x => candidateGraphs.contains(x._1))
      println(candidateGraphs)
    }
    return candidateGraphs
  }


  /**
    *
    * @param queryGraph
    * @param dGTreeRoot
    */
  def superGraphSearch(queryGraph:Graph[String, String],
                       dGTreeRoot: DGTreeNode): mutable.Set[Long] ={

    val heap = new mutable.PriorityQueue[QueryNode]()(Ord)
    // val heap = QueryProcessor.heap
    //var resultantSubGraphs: mutable.Map[Long, Graph[String, String]] = new mutable.HashMap[Long, Graph[String, String]]()    //A(Q)
    //var resultantSubGraphs: mutable.HashSet[Long] = new mutable.HashSet[Long]()

    //finding the valid datagraphs in the database
    var candidateDGs = dGTreeRoot.s.filter(graph => {
      (graph._2.edges.count() <= queryGraph.edges.count())&&
        (graph._2.vertices.count() <= queryGraph.vertices.count())
    })

    var queryNode = new QueryNode
    queryNode.treeNode = dGTreeRoot
    queryNode.sStar = candidateDGs

    //filling up matches from the query graph
    val vList = queryGraph.vertices.map(vertex => {
      val vertexList: ArrayBuffer[(String, Long)] = new ArrayBuffer[(String, Long)]
      vertexList.+=((vertex._2, vertex._1.toLong))
      ArrayBuffer(vertexList)
    })
    queryNode.matches(queryGraphId) = vList             //TODO: converted the value part from ListBuffer to RDD[ListBuffer...
    println("matches done for graph id "+queryGraphId)
    //*** vList.foreach(arrbuf=>arrbuf.foreach(println))

    calculateScore(queryNode)

    heap += queryNode

    while(candidateDGs.nonEmpty) {
      queryNode = bestFeature(heap, candidateDGs)
      if (queryNode != null) {
        val dGTreeNode = queryNode.treeNode

        dGTreeNode.children.foreach(gPlus => {
          if (gPlus.children.isEmpty) {
            //dgtree indexing reached to the datagraphs by analyzing each edge in the graph and now gplus with fgraph=datagrah doesnot have any growedge
            // if(gPlus.growEdge.edges.isEmpty()){
            if (gPlus.s.size > 1) { //i think this should go away
              println("there are more than one grpah in gPlus")
              val candidateList = featureExpansion(queryGraphId, queryGraph, queryNode, gPlus, heap, candidateDGs, 0) //TODO: 0 and 1 indicates if it is leaf node or not, change it to some better variable
              //candidateDGs = candidateDGs.filter(x => candidateList.contains(x._1))       //TODO:these two lines can be merged
            } else {
              //TODO: some checking is required for labelled graphs, check with single edge graph
              //resultantSubGraphs += gPlus.s.head._1
              val candidateList = featureExpansion(queryGraphId, queryGraph, queryNode, gPlus, heap, candidateDGs, 0)
              //candidateDGs = candidateDGs.filter(x => candidateList.contains(x._1))       //TODO:these two lines can be merged
            }
            /* }else{
             //dgtree indexing has abruptly end due to a unique edge found in the data graph. This node gPlus will have a grow edge.
             featureExpansion(queryGraphId, queryGraph, queryNode, gPlus, heap, candidateDGs, 0, resultantSubGraphs)   //TODO: 0 and 1 indicates if it is leaf node or not, change it to some better variable
           }*/
            candidateDGs = candidateDGs.filter(x => !gPlus.sStar.contains(x._1))
          } else {
            val candidateList = featureExpansion(queryGraphId, queryGraph, queryNode, gPlus, heap, candidateDGs, 1)
            candidateDGs = candidateDGs.filter(x => candidateList.contains(x._1))       //TODO:these two lines can be merged
          }
        })
      }else{
        candidateDGs = candidateDGs.empty
      }
    }
    return resultantSubGraphs
  }
}
