package com.stratio.deep.extractor.client

import com.stratio.deep.rdd.IExtractor
import scala.reflect.ClassTag
import akka.actor.Actor
import com.stratio.deep.config.ExtractorConfig
import com.stratio.deep.extractor.message.NextResponse
import com.stratio.deep.extractor.message.HasNextResponse
import com.stratio.deep.extractor.message.GetPartitionsAction
import com.stratio.deep.extractor.message.GetPartitionsResponse
import com.stratio.deep.extractor.message.CloseAction
import com.stratio.deep.extractor.message.CloseResponse
import com.stratio.deep.extractor.message.InitIteratorResponse
import com.stratio.deep.extractor.message.InitIteratorAction
import com.stratio.deep.extractor.message.HasNextAction
import com.stratio.deep.extractor.message.NextAction

/**
 * Created by darroyo on 22/08/14.
 */
class ExtractorActor[T: ClassTag] extends Actor { //with ActionSystemMessage{

  protected var extractorA: IExtractor[T] = null

  println("Starting actor!")

  def receive = {


    case CloseAction() =>
      println("message received")
      extractorA.close()
      sender ! new CloseResponse(true)


    case GetPartitionsAction(config) =>
      println("message received")

      if (extractorA == null) {
        val rdd: Class[T] = config.getExtractorImplClass.asInstanceOf[Class[T]]
        try {
          extractorA = rdd.newInstance().asInstanceOf[IExtractor[T]]

        }
        catch {
          case e: Any => {
            e.printStackTrace
          }
        }

      }
      sender ! new GetPartitionsResponse(extractorA.getPartitions(config.asInstanceOf[ExtractorConfig[T]]));


    case  HasNextAction() =>
      println("message received")
      sender ! new HasNextResponse(extractorA.hasNext)



    case InitIteratorAction(partition,config) =>
      println("message received")
      if (extractorA == null) {
        val rdd: Class[T] = config.getExtractorImplClass.asInstanceOf[Class[T]]
        try {
          extractorA = rdd.newInstance().asInstanceOf[IExtractor[T]]

        }
        catch {
          case e: Any => {
            e.printStackTrace
          }
        }

      }
      extractorA.initIterator(partition, config.asInstanceOf[ExtractorConfig[T]])

      sender ! new InitIteratorResponse(true);

    case NextAction() =>
     sender ! new NextResponse(extractorA.next().asInstanceOf[T])
     // this.next()
     // sender() ! DivisionResult(n1, n2, n1 / n2)
  }



}
