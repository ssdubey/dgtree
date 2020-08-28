/*
package dgtree

import org.apache.spark.graphx.Graph

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.PriorityQueue

class construct (graphDB: mutable.Map[Long, Graph[String, String]]) {


  def runPhase1(node: DGTreeNode, heap: PriorityQueue[DGTreeNode]): PriorityQueue[DGTreeNode] = {

    heap
  }

  def candidateFeature(node: DGTreeNode) = {
    var heap = new mutable.PriorityQueue[DGTreeNode]()

    heap = runPhase1(node,heap)

    heap = runPhase2(node, heap)

    heap = calculateScore(heap)

    return heap
  }

  def treeGrow(node: DGTreeNode)={
    //val candidateHeap: mutable.PriorityQueue[DGTreeNode] = mutable.PriorityQueue.empty[DGTreeNode]
    candidateFeature(node)
  }

  /**
    * This method will create a new root node with values of s, s* and matches(possible extensions)
    * Further it will call the treeGrow method which will further extend the dgTree
  def constructDGTree(): DGTreeNode = {

    //create a new root node and initialize it
    val root = new DGTreeNode
    root.s = graphDB
    root.sStar = graphDB

    //putting all vertices of each graph as a match for the root node
    //var matches:Map[Long, List[Long]] = null
    graphDB.foreach(graph => {
      val graphId = graph._1 //key in the map

      val vertexList: ArrayBuffer[Long] = new ArrayBuffer[Long]

      val vList = graph._2.vertices.map(vertex => {
        vertexList.clear()                      //TODO:vertexList was getting prepared in a wierd way, so had to clear it
        //every time. correctness is not sure yet.
        vertexList.+=(vertex._1.toLong)
        vertexList
      })
      root.matches(graphId) = vList             //TODO: converted the value part from ListBuffer to RDD[ListBuffer...
      //TODO: check here for any related error
      treeGrow(root)
    })
    root                                        //returning root to the main code for query processing
  }
}*/
*/
