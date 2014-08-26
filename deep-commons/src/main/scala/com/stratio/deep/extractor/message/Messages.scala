package com.stratio.deep.extractor.message

import com.stratio.deep.config.ExtractorConfig
import com.stratio.deep.rdd.DeepTokenRange
import scala.reflect.ClassTag
import org.apache.spark.Partition

// Messages the server can handle
sealed trait ActionSystemMessage

case class CloseAction() extends ActionSystemMessage

case class GetPartitionsAction[T: ClassTag](config: ExtractorConfig[T]) extends ActionSystemMessage

case class HasNextAction() extends ActionSystemMessage

case class InitIteratorAction[T: ClassTag](partition: Partition, config: ExtractorConfig[T]) extends ActionSystemMessage

case class NextAction() extends ActionSystemMessage

// Messages the client can handle 
sealed trait ResponseSystemMessage

case class CloseResponse(isClosed: Boolean) extends ResponseSystemMessage

case class GetPartitionsResponse(partitions: Array[Partition]) extends ResponseSystemMessage

case class HasNextResponse(hasNext: Boolean) extends ResponseSystemMessage

case class InitIteratorResponse(isInitialized: Boolean) extends ResponseSystemMessage

case class NextResponse[T: ClassTag](data: T) extends ResponseSystemMessage
