package uwt.socialnetworks
import org.apache.spark.graphx.VertexId

class Message (msgId:VertexId, msgSrc:VertexId, msgDst:VertexId) extends Serializable{
	var messageId:VertexId = msgId
	var content:MessageContent = new MessageContent(msgId, msgSrc, msgDst)
	var messages:List[MessageContent] = List[MessageContent](content)
	/*var srcId:VertexId = msgSrc
	var dstId:VertexId = msgDst
	var level:Int = -1
	var credit:Double = 0
	var shortestPathCount:Int = -1
	//var isAggregator:Boolean = false
	var isBackwardMessage:Boolean = false
	override def toString() =
	{
		var msgs = "messages=[";
		messages.foreach(i=>msgs+= "(msgId="+i.messageId+", src="+i.srcId+", dst="+i.dstId+",lvl="+i.level+",paths="+i.shortestPathCount+",credit="+i.credit+",isBackward="+i.isBackwardMessage+")")
		msgs+="]"
		"(msgId="+messageId+", src="+srcId+", dst="+dstId+",lvl="+level+",paths="+shortestPathCount+",credit="+credit+",isBackward="+isBackwardMessage+"; "+msgs+")"
	
	}*/
	
	override def toString() = "msgId="+messageId+ "; messages=["+messages+"]"
}