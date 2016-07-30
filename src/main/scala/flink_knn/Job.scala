package flink_knn

import java.io.File

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import flink_knn.Utils._
import flink_knn.Utils.DatasetType._
import flink_knn.Utils.MetricDistance.MetricDistance
import org.apache.flink.api.common.functions.{GroupReduceFunction, RichMapPartitionFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.apache.flink.ml.metrics.distances.{EuclideanDistanceMetric, TanimotoDistanceMetric}
import org.apache.flink.util.Collector
import java.lang.Iterable
import java.io.File

import breeze.linalg.{DenseVector => BreezeDenseVector, Vector => BreezeVector}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.collection.mutable

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   mvn clean package
 * }}}
 * in the projects root directory. You will find the jar in
 * target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    //Usage: [-k num] [-m euclidean | gower] [-in path_to_input] [-outputPath path_to_output] [-s (itemtoParse)] [-set susy |thyroid]
    val parameters : ParameterTool = ParameterTool.fromArgs(args)
    val k_from_args: Int = parameters.getInt("k")
    val metric: MetricDistance = ParsingUtils.parseDistance(parameters.get("m","euclidean"))
    val item: String = parameters.getRequired("s")
    val inputPath: String = parameters.getRequired("in")
    val outputPath: String = parameters.getRequired("outputPath")
    val dataset2Use : DatasetType = ParsingUtils.parseDataset(parameters.getRequired("set"))

    val testInstance: DenseVector = {
      if(dataset2Use==DatasetType.SUSY)
        ParsingUtils.parseSUSYInstanceCSV(item)
      else
        ParsingUtils.parseThyroidInstanceCSV(item)
    }

    val prefix_localPath= "file://"
    val trainingSet_toParse= env.readTextFile(prefix_localPath+inputPath)

    val trainingSet: DataSet[LabeledVector]= {
      if(dataset2Use== DatasetType.SUSY)
        trainingSet_toParse.map(ParsingUtils.parseSUSYLabelledInstance(_))
      else
        trainingSet_toParse.map(ParsingUtils.parseThyroidLabelledInstance(_))
    }

    def min(a:Double, b: Double): Double =  if(a < b) a else b
    def max(a: Double, b: Double): Double = if(a > b) a else b



    def extractFeatureMinMaxVectors(dataSet: DataSet[(BreezeDenseVector[Double], BreezeDenseVector[Double])])
    : DataSet[(BreezeDenseVector[Double], BreezeDenseVector[Double])] = {
      //val minMax_dense: DataSet[(BreezeDenseVector[Double], BreezeDenseVector[Double])] = dataSet.map{v =>
      //val minMax_dense = dataSet.map(v =>(BreezeDenseVector(v.data), BreezeDenseVector(v.data)))
      dataSet.reduce{
        (minMax1, minMax2) => {
          val tempMinimum = breeze.linalg.min(minMax1._1, minMax2._1)
          val tempMaximum = breeze.linalg.max(minMax1._2, minMax2._2)
          (tempMinimum, tempMaximum)
        }
      }
    }

    //We want to read the range from file
    //val resourcesPath = getClass.getResource("range.csv")
    //println(resourcesPath.getPath)
    val currentPath= new File(".").getAbsolutePath()
    val rangeFile= if(dataset2Use==DatasetType.SUSY) new File(currentPath+"range_susy.csv") else new File(currentPath+"range_thyroid.csv")
    val rangeVectorFromFile: Option[DenseVector]= if(rangeFile.exists()){
      val vectData: DenseVector= env.readTextFile(rangeFile.getPath).map(ParsingUtils.parseSUSYInstanceCSV(_)).collect().seq(0)
      Some(vectData)
    } else None



    val range: Option[DenseVector] =
      if(metric== MetricDistance.GOWER){
        if(rangeVectorFromFile.isDefined)
          Some(rangeVectorFromFile.get)
        else{
          //Calculate range iff the metric in use is the Gower
          val onlyVector = trainingSet.map { x =>
            val vector = x.vector
            vector match {
              case vector: SparseVector => (BreezeDenseVector(vector.toDenseVector.data), BreezeDenseVector(vector.toDenseVector.data))
              case vector: DenseVector => (BreezeDenseVector(vector.data), BreezeDenseVector(vector.data))
            }
          }
          val minMax: DataSet[(BreezeDenseVector[Double], BreezeDenseVector[Double])] = extractFeatureMinMaxVectors(onlyVector)
          val minimum = minMax.collect().seq(0)._1
          val maximum = minMax.collect().seq(0)._2
          val dense = DenseVector((maximum - minimum).toArray)
          Some(dense)
        }
      } else None

    case class DistanceWithLabel(dist: Double, label: Double) extends Ordered[DistanceWithLabel] {
      //import scala.math.Ordered.orderingToOrdered
      def compare(that: DistanceWithLabel): Int = this.dist compare that.dist
    }

    val orderByDistance = Ordering.by[DistanceWithLabel, Double](_.dist)

    case class Args2Broadcast(value4k:Int, testInstance: DenseVector, metricDistance: MetricDistance)

    //It reminds java because...essentially it is! See import of import java.lang.Iterable
    class mapWithQueue extends RichMapPartitionFunction[LabeledVector,DistanceWithLabel]{
      private var maxK: Int = 0
      private var testInstance: DenseVector = null
      private var metric2Use : MetricDistance = null

      override def open(parameters: Configuration) {
        val passedParameters= getRuntimeContext().getBroadcastVariable[Args2Broadcast]("kUserMetric").get(0)
        maxK= passedParameters.value4k
        testInstance= passedParameters.testInstance
        metric2Use= passedParameters.metricDistance
      }

      override def mapPartition(iterable: Iterable[LabeledVector], collector: Collector[DistanceWithLabel]): Unit = {
        val prioritykQueue= mutable.PriorityQueue[DistanceWithLabel]()(orderByDistance)
        val record_iterator= iterable.iterator
        val metric= if(metric2Use==MetricDistance.EUCLIDEAN) EuclideanDistanceMetric() else GowerDistance(range.get)
        var minItem: Option[DistanceWithLabel] = None

        while(record_iterator.hasNext){
          val in: LabeledVector = record_iterator.next()
          val currentInstance: DistanceWithLabel = {
            val dist:Double = metric.distance(in.vector, testInstance)
            new DistanceWithLabel(dist,in.label)
          }
          if (k_from_args == 1) {
            if (minItem.isEmpty)
              minItem = Some(currentInstance)
            else {
              if (minItem.get.dist > currentInstance.dist)
                minItem = Some(currentInstance)
            }
          }
          else {
            if (prioritykQueue.size <= maxK)
              prioritykQueue += currentInstance

            else {
              if (prioritykQueue.head.dist > currentInstance.dist) {
                prioritykQueue.dequeue()
                prioritykQueue += currentInstance
              }
            }
          }
        }
        if(k_from_args==1)
          collector.collect(minItem.get)
        else{
          while(!prioritykQueue.isEmpty)
            collector.collect(prioritykQueue.dequeue())
        }
      }
    }

    class reduceWithQueue extends GroupReduceFunction[DistanceWithLabel,DistanceWithLabel]{
      override def reduce(iterable: Iterable[DistanceWithLabel], collector: Collector[DistanceWithLabel]): Unit = {
        val prioritykQueue= mutable.PriorityQueue[DistanceWithLabel]()(orderByDistance)
        val record_iterator= iterable.iterator()
        var minItem: Option[DistanceWithLabel] = None

        while(record_iterator.hasNext){
          val in:DistanceWithLabel= record_iterator.next()
          if(k_from_args==1){
            if(minItem.isEmpty)
              minItem= Some(in)
            else{
              if(minItem.get.dist> in.dist)
                minItem= Some(in)
            }
          }
          else{
            if(prioritykQueue.size <= k_from_args)
              prioritykQueue+= in
            else{
              if(prioritykQueue.head.dist > in.dist){
                prioritykQueue.dequeue()
                prioritykQueue+= in
              }
            }
          }
        }
        if(k_from_args==1)
          collector.collect(minItem.get)
        else{
          while(!prioritykQueue.isEmpty)
            collector.collect(prioritykQueue.dequeue())
        }
      }
    }

    val toBroadcast: DataSet[Args2Broadcast] = env.fromElements(Args2Broadcast(k_from_args,testInstance,metric))

    /*
    val TopK_local : DataSet[DistanceWithLabel]={
      val metric2=
        if(metric==MetricDistance.EUCLIDEAN) EuclideanDistanceMetric() else GowerDistance(range.get)

      trainingSet.map{x =>
          val dist:Double = metric2.distance(x.vector, testInstance)
          new DistanceWithLabel(dist,x.label)
        }
    }*/

    val TopK_local : DataSet[DistanceWithLabel]=
        trainingSet.mapPartition(new mapWithQueue).withBroadcastSet(toBroadcast, "kUserMetric")

    val TopK: DataSet[DistanceWithLabel]= env.fromCollection(TopK_local.reduceGroup(new reduceWithQueue).collect())
    val sortedTopK= TopK.map(x => (x.dist, x.label)).groupBy(0).sortGroup(0,Order.ASCENDING).first(k_from_args)
    //Adding overwrite such that if there are existing files it overwrites them without making the job fail
    sortedTopK.writeAsCsv(prefix_localPath+outputPath,"\n",",", WriteMode.OVERWRITE).setParallelism(1)

    /*
    val resultFile= new File(tempFilePath)
    if(resultFile.exists()){
      import scala.io.Source
      val bufferedSource = Source.fromFile(resultFile)
      var classLabel: Option[String] = None
      for (line <- bufferedSource.getLines) {
        println(line)
        classLabel= Some(line)
      }
      bufferedSource.close*/

    if(range.isDefined && rangeVectorFromFile.isEmpty)
      env.fromCollection(range).map{ x =>
        x.data.map(y => f"$y%e").mkString(",")
      }.writeAsText("file://"+rangeFile.getPath, WriteMode.OVERWRITE).setParallelism(1)
    //Write to output

    env.execute("KNN-Flink")
  }
}
