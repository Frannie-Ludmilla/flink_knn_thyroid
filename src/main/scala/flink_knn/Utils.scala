package flink_knn.Utils

import breeze.linalg
import breeze.linalg.{Vector => BreezeVector, DenseVector => BreezeDenseVector}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.math.{DenseVector, SparseVector, Vector => MLVector}
import org.apache.flink.ml.metrics.distances.DistanceMetric

object MetricDistance extends Enumeration {
  type MetricDistance = Value
  val GOWER, EUCLIDEAN = Value
}

object DatasetType extends Enumeration {
  type DatasetType = Value
  val SUSY, THYROID = Value
}

object ParsingUtils{
  /**
    * Note that the DenseVector here comes from Breeze.linalg
    * */
  import org.apache.flink.ml.common.LabeledVector
  import flink_knn.Utils.DatasetType.DatasetType
  import flink_knn.Utils.MetricDistance.MetricDistance

  def parseThyroidLabelledInstance(item: String): LabeledVector = {
    val arrDouble = item.split(" ").map(_.toDouble)
    new LabeledVector(arrDouble(21), DenseVector(arrDouble.slice(0,21)))
  }

  def parseThyroidInstanceCSV(item: String): DenseVector = {
    new DenseVector(item.split(",").map(_.toDouble))
  }

  def parseSUSYLabelledInstance(item: String): LabeledVector ={
    val arrayDouble= item.split(",").map(BigDecimal(_).toDouble)
    new LabeledVector(arrayDouble(0), DenseVector(arrayDouble.slice(1,arrayDouble.length)))
  }

  def parseSUSYInstanceCSV(item: String): DenseVector = {
    new DenseVector(item.split(",").map(BigDecimal(_).toDouble))
  }

  def parseDistance(str: String): MetricDistance = str match {
    case s if s matches "(?i)euclidean" => MetricDistance.EUCLIDEAN
    case s if s matches "(?i)gower" => MetricDistance.GOWER
    case _ => MetricDistance.EUCLIDEAN
  }

  def parseDataset(string: String): DatasetType = string match  {
    case s if s matches "(?i)susy" => DatasetType.SUSY
    case s if s matches "(?i)thyroid" => DatasetType.THYROID
  }



}

class GowerDistance(rangePar: DenseVector) extends DistanceMetric {
  val range= rangePar

  override def distance(a: MLVector, b: MLVector) = {
    var result, gowenCoeff = 0.0
    val sizeFirst= a.size
    if (sizeFirst == b.size) {
      var index = 0
      gowenCoeff = 0.0
      while (index < sizeFirst) {
        gowenCoeff += math.abs(a(index) - b(index))/range(index)
        index += 1
      }
      result = gowenCoeff /sizeFirst
    }
    else result = -1
    result
    }
}

object GowerDistance {
  def apply(range: DenseVector) = new GowerDistance(range)
}




/**
def gowenDissimilarity(v1: DenseVector, v2: DenseVector, r: DenseVector): Double = {
      var result, gowenCoeff = 0.0
      if (v1.size == v2.size) {
        var index = 0
        gowenCoeff = 0.0
        while (index < v1.size) {
          gowenCoeff += math.abs(v1(index) - v2(index)) / r(index)
          index += 1
        }
        //result= math.sqrt((1.0/r.toArray.sum)*gowenCoeff)
        result = gowenCoeff / v1.size
      }
      else result = -1
      result
    }
  *
  * */