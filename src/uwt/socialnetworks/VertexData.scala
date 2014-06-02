package uwt.socialnetworks
import collection.immutable.HashMap
import org.apache.spark.graphx.VertexId
import uwt.socialnetworks.Message

class VertexData extends Serializable{
	var messages:HashMap[VertexId,Message] = new HashMap()
	var levels:scala.collection.mutable.Map[VertexId,Int] =  scala.collection.mutable.Map[VertexId,Int]()
	var shortestPaths:scala.collection.mutable.Map[VertexId,Int] =  scala.collection.mutable.Map[VertexId,Int]()
	var credits:scala.collection.mutable.Map[VertexId,Double] =  scala.collection.mutable.Map[VertexId,Double]()
	var isLeaf:scala.collection.mutable.Map[VertexId,Boolean] =  scala.collection.mutable.Map[VertexId,Boolean]()
	var isCreditPropegated:scala.collection.mutable.Map[VertexId,Boolean] =  scala.collection.mutable.Map[VertexId,Boolean]()
	var numOfMessagesSent:scala.collection.mutable.Map[VertexId,Int] =  scala.collection.mutable.Map[VertexId,Int]()
	override def toString() = "(paths="+shortestPaths+", levels="+levels+", credits="+credits+", isLeaf="+isLeaf+", numOfMessagesSent="+numOfMessagesSent+")"
	//override def toString() = "(credits="+credits+",  numOfMessagesSent="+numOfMessagesSent+",  isCreditPropegated="+isCreditPropegated+")"

}