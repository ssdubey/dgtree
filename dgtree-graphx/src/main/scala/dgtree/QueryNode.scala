package dgtree

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *
  */
class QueryNode {

  var treeNode: DGTreeNode = new DGTreeNode
  var sStar:mutable.Map[Long, Graph[String, String]] = new mutable.HashMap[Long, Graph[String, String]]()
  var matches:mutable.Map[Long, RDD[ArrayBuffer[ArrayBuffer[(String, Long)]]]] = new mutable.HashMap[Long, RDD[ArrayBuffer[ArrayBuffer[(String, Long)]]]]()
  var score:Double = 0
}
