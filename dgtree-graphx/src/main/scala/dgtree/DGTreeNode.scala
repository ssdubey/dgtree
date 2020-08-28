package dgtree

import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Set, mutable}

/**
  * Structure of a DGTree node. Each node will be an object of this class
  * matches: it is a map keyed with graphId. for each graph, it keeps the vertexIds(as in the graph) for the vertices present in the feature graph of this node.
  * vertices are kept at the index corresponding to vertexId of that vertex in feature graph.
  */
class DGTreeNode(){                                   //TODO:consider changing all nulls to _

  var children:ListBuffer[DGTreeNode] = new ListBuffer[DGTreeNode]()
  var featureGraph:Graph[String, String] = Graph(Index.sc.emptyRDD, Index.sc.emptyRDD)
  var growEdge:Graph[String, String] = Graph(Index.sc.emptyRDD, Index.sc.emptyRDD)                       //TODO:to be seen how entry is done for this
  var edgeType:String = null                          //EdgeType enum TODO: change the type to EdgeType
  var s:mutable.Map[Long, Graph[String, String]] = new mutable.HashMap[Long, Graph[String, String]]()                             //List of graph Ids
  var matches:mutable.Map[Long, RDD[ArrayBuffer[ArrayBuffer[(String, Long)]]]] = new mutable.HashMap[Long, RDD[ArrayBuffer[ArrayBuffer[(String, Long)]]]]()          //key is the graph id and value is the list of vertices
  //var matches:mutable.Map[Long, RDD[ArrayBuffer[(String, Long)]]] = new mutable.HashMap[Long, RDD[ArrayBuffer[(String, Long)]]]()          //key is the graph id and value is the list of vertices
  var sStar:mutable.Map[Long, Graph[String, String]] = new mutable.HashMap[Long, Graph[String, String]]()                        //List of graph Ids
  var score:Double = 0

}
