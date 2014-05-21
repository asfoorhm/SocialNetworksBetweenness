package uwt.socialnetworks
import org.apache.spark.graphx.VertexId

class Message (msgId:VertexId, msgSrc:VertexId, msgDst:VertexId) extends Serializable{
	var messageId:VertexId = msgId
	var messages:List[Message] = List[Message](this)
	var srcId:VertexId = msgSrc
	var dstId:VertexId = msgDst
	var level:Int = -1
	var credit:Double = 0
	var shortestPathCount:Int = -1
	
	var isBackwardMessage:Boolean = false
	override def toString() = "(msgId="+messageId+", src="+srcId+", dst="+dstId+",lvl="+level+",paths="+shortestPathCount+",credit="+credit+")"

}