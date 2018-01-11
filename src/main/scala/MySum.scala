import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders

case class Sum(var sum: Long)

object MySum extends Aggregator[Student, Sum, Double] {

  def zero: Sum = Sum(0L)

  def reduce(buffer: Sum, student: Student): Sum = {
    buffer.sum += student.age
    buffer
  }

  def merge(b1: Sum, b2: Sum): Sum = {
    b1.sum += b2.sum
    b1
  }

  def finish(res: Sum): Double = res.sum

  def bufferEncoder: Encoder[Sum] = Encoders.product

  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
