
package org.apache.spark.scheduler



import org.apache.spark.ExceptionFailure
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.SparkContext
import org.apache.spark.Success
import org.apache.spark.ui.jobs.ExecutorSummary
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.Seq
import scala.collection.mutable.{ListBuffer, HashMap, HashSet}

import org.apache.spark.{ExceptionFailure, SparkContext, Success}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.Logging


private[spark] class SdnJudger() extends SparkListener with Logging{

  // server talk with sdn controller
  val myServer = null //NettySever


  // How many stages to remember
  val RETAINED_STAGES = 1000 //sc.conf.getInt("spark.ui.retainedStages", 1000)
  val DEFAULT_POOL_NAME = "default"

  val stageIdToPool = new HashMap[Int, String]()
  val stageIdToDescription = new HashMap[Int, String]()
  val poolToActiveStages = new HashMap[String, HashSet[StageInfo]]()

  val activeStages = HashSet[StageInfo]()
  val completedStages = ListBuffer[StageInfo]()
  val failedStages = ListBuffer[StageInfo]()

  var startPredict = false

  // Total metrics reflect metrics only for completed tasks
  var totalTime = 0L
  var totalShuffleRead = 0L
  var totalShuffleWrite = 0L

  val stageIdToTime = HashMap[Int, Long]()
  val stageIdToShuffleRead = HashMap[Int, Long]()
  val stageIdToShuffleWrite = HashMap[Int, Long]()
  val stageIdToMemoryBytesSpilled = HashMap[Int, Long]()
  val stageIdToDiskBytesSpilled = HashMap[Int, Long]()
  val stageIdToTasksActive = HashMap[Int, HashSet[TaskInfo]]()
  val stageIdToTasksComplete = HashMap[Int, Int]()
  val stageIdToTasksFailed = HashMap[Int, Int]()
  val stageIdToTaskInfos =
    HashMap[Int, HashSet[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]]()
  val stageIdToExecutorSummaries = HashMap[Int, HashMap[String, ExecutorSummary]]()

  var stageNames = HashMap[String, ListBuffer[Long]]()

  override def onJobStart(jobStart: SparkListenerJobStart) {}

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = synchronized {
    val stage = stageCompleted.stage
    poolToActiveStages(stageIdToPool(stage.stageId)) -= stage
    activeStages -= stage
    completedStages += stage
    trimIfNecessary(completedStages)

    //有多个可以设置的了
    val condition = checkPredictCondition()
    if(condition) {
      startPredict = true
      logInfo("start prediction")
    }
  }

  def checkPredictCondition() = {
    //根据task的函数名进行划分，当每个函数执行的次数都超过了3次，则返回true, 否则返回false
    var res = true
    synchronized{
      val tmp = completedStages.sortBy(_.submissionTime)
      for(t <- tmp){
        val start_time = t.submissionTime getOrElse 0L
        var end_time = t.completionTime getOrElse 0L

        if(stageNames.contains(t.name)){
          stageNames(t.name).append(end_time - start_time)
        }
        else{
          stageNames(t.name) = ListBuffer(end_time - start_time)
        }
        logInfo(t.name + "@@@")
        if(stageNames(t.name).length < 3)
          res = false

      }
    }
    res

  }

  /** If stages is too large, remove and garbage collect old stages */
  def trimIfNecessary(stages: ListBuffer[StageInfo]) = synchronized {
    if (stages.size > RETAINED_STAGES) {
      val toRemove = RETAINED_STAGES / 10
      stages.takeRight(toRemove).foreach( s => {
        stageIdToTaskInfos.remove(s.stageId)
        stageIdToTime.remove(s.stageId)
        stageIdToShuffleRead.remove(s.stageId)
        stageIdToShuffleWrite.remove(s.stageId)
        stageIdToMemoryBytesSpilled.remove(s.stageId)
        stageIdToDiskBytesSpilled.remove(s.stageId)
        stageIdToTasksActive.remove(s.stageId)
        stageIdToTasksComplete.remove(s.stageId)
        stageIdToTasksFailed.remove(s.stageId)
        stageIdToPool.remove(s.stageId)
        if (stageIdToDescription.contains(s.stageId)) {stageIdToDescription.remove(s.stageId)}
      })
      stages.trimEnd(toRemove)
    }
  }

  /** For FIFO, all stages are contained by "default" pool but "default" pool here is meaningless */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = synchronized {
    val stage = stageSubmitted.stage
    activeStages += stage

    val poolName = Option(stageSubmitted.properties).map {
      p => p.getProperty("spark.scheduler.pool", DEFAULT_POOL_NAME)
    }.getOrElse(DEFAULT_POOL_NAME)
    stageIdToPool(stage.stageId) = poolName

    val description = Option(stageSubmitted.properties).flatMap {
      p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }
    description.map(d => stageIdToDescription(stage.stageId) = d)

    val stages = poolToActiveStages.getOrElseUpdate(poolName, new HashSet[StageInfo]())
    stages += stage

    if(startPredict) {
         logInfo("start notify the sdn controller")
         val res = stageNames(stage.name)
         if(res == None){
           startPredict = false
           stageNames = HashMap[String, ListBuffer[Long]]()
           logInfo("finished predict")
         }
         //myServer()
         logInfo("we notified the controller, adjsut the network after " + res(0) )

    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) = synchronized {
    val sid = taskStart.task.stageId
    val tasksActive = stageIdToTasksActive.getOrElseUpdate(sid, new HashSet[TaskInfo]())
    tasksActive += taskStart.taskInfo
    val taskList = stageIdToTaskInfos.getOrElse(
      sid, HashSet[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]())
    taskList += ((taskStart.taskInfo, None, None))
    stageIdToTaskInfos(sid) = taskList
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult)
  = synchronized {
    // Do nothing: because we don't do a deep copy of the TaskInfo, the TaskInfo in
    // stageToTaskInfos already has the updated status.
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val sid = taskEnd.task.stageId

    // create executor summary map if necessary
    val executorSummaryMap = stageIdToExecutorSummaries.getOrElseUpdate(key = sid,
      op = new HashMap[String, ExecutorSummary]())
    executorSummaryMap.getOrElseUpdate(key = taskEnd.taskInfo.executorId,
      op = new ExecutorSummary())

    val executorSummary = executorSummaryMap.get(taskEnd.taskInfo.executorId)
    executorSummary match {
      case Some(y) => {
        // first update failed-task, succeed-task
        taskEnd.reason match {
          case Success =>
            y.succeededTasks += 1
          case _ =>
            y.failedTasks += 1
        }

        // update duration
        y.taskTime += taskEnd.taskInfo.duration

        Option(taskEnd.taskMetrics).foreach { taskMetrics =>
          taskMetrics.shuffleReadMetrics.foreach { y.shuffleRead += _.remoteBytesRead }
          taskMetrics.shuffleWriteMetrics.foreach { y.shuffleWrite += _.shuffleBytesWritten }
          y.memoryBytesSpilled += taskMetrics.memoryBytesSpilled
          y.diskBytesSpilled += taskMetrics.diskBytesSpilled
        }
      }
      case _ => {}
    }

    val tasksActive = stageIdToTasksActive.getOrElseUpdate(sid, new HashSet[TaskInfo]())
    tasksActive -= taskEnd.taskInfo

    val (failureInfo, metrics): (Option[ExceptionFailure], Option[TaskMetrics]) =
      taskEnd.reason match {
        case e: ExceptionFailure =>
          stageIdToTasksFailed(sid) = stageIdToTasksFailed.getOrElse(sid, 0) + 1
          (Some(e), e.metrics)
        case _ =>
          stageIdToTasksComplete(sid) = stageIdToTasksComplete.getOrElse(sid, 0) + 1
          (None, Option(taskEnd.taskMetrics))
      }

    stageIdToTime.getOrElseUpdate(sid, 0L)
    val time = metrics.map(m => m.executorRunTime).getOrElse(0)
    stageIdToTime(sid) += time
    totalTime += time

    stageIdToShuffleRead.getOrElseUpdate(sid, 0L)
    val shuffleRead = metrics.flatMap(m => m.shuffleReadMetrics).map(s =>
      s.remoteBytesRead).getOrElse(0L)
    stageIdToShuffleRead(sid) += shuffleRead
    totalShuffleRead += shuffleRead

    stageIdToShuffleWrite.getOrElseUpdate(sid, 0L)
    val shuffleWrite = metrics.flatMap(m => m.shuffleWriteMetrics).map(s =>
      s.shuffleBytesWritten).getOrElse(0L)
    stageIdToShuffleWrite(sid) += shuffleWrite
    totalShuffleWrite += shuffleWrite

    stageIdToMemoryBytesSpilled.getOrElseUpdate(sid, 0L)
    val memoryBytesSpilled = metrics.map(m => m.memoryBytesSpilled).getOrElse(0L)
    stageIdToMemoryBytesSpilled(sid) += memoryBytesSpilled

    stageIdToDiskBytesSpilled.getOrElseUpdate(sid, 0L)
    val diskBytesSpilled = metrics.map(m => m.diskBytesSpilled).getOrElse(0L)
    stageIdToDiskBytesSpilled(sid) += diskBytesSpilled

    val taskList = stageIdToTaskInfos.getOrElse(
      sid, HashSet[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]())
    taskList -= ((taskEnd.taskInfo, None, None))
    taskList += ((taskEnd.taskInfo, metrics, failureInfo))
    stageIdToTaskInfos(sid) = taskList
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) = synchronized {
    jobEnd match {
      case end: SparkListenerJobEnd =>
        end.jobResult match {
          case JobFailed(ex, Some(stage)) =>
            /* If two jobs share a stage we could get this failure message twice. So we first
            *  check whether we've already retired this stage. */
            val stageInfo = activeStages.filter(s => s.stageId == stage.id).headOption
            stageInfo.foreach {s =>
              activeStages -= s
              poolToActiveStages(stageIdToPool(stage.id)) -= s
              failedStages += s
              trimIfNecessary(failedStages)
            }
          case _ =>
        }
      case _ =>
    }
  }

  def sendPriority(){

  }
}




