import java.util.Properties

import org.apache.spark.sql.SparkSession

case class Student (id: Int, firstName: String, lastName: String, age: Int)
case class Book (id: Int, name: String, student_id: Int)

object SparkApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "")

    val jdbcUrl = "jdbc:mysql://localhost:3306/test"

    val students = spark.read.jdbc(jdbcUrl, "student", connectionProperties).as[Student]
    val books = spark.read.jdbc(jdbcUrl, "book", connectionProperties).as[Book]

    students.show()
//    +---+---------+---------+---+
//    | id|firstName| lastName|age|
//    +---+---------+---------+---+
//    |  2|   Serhii|    Bzdel| 22|
//    |  3|     Vova|   Tysjak| 23|
//    |  4|   Andrii|    Savka| 21|
//    |  5|    Taras|Petrushak| 23|
//    |  6|    Nazar|     Pron| 25|
//    +---+---------+---------+---+

    books.show()
//    +---+---------------+----------+
//    | id|           name|student_id|
//    +---+---------------+----------+
//    |  2|  Harry Poter 1|         2|
//    |  3|  Harry Poter 2|         3|
//    |  4|  Harry Poter 3|         4|
//    |  5|  Harry Poter 4|         5|
//    |  6|  Harry Poter 5|         6|
//    |  7|  Harry Poter 6|         2|
//    |  8|Harry Poter 7_1|         3|
//    |  9|Harry Poter 7_2|         4|
//    +---+---------------+----------+

    val studentsWithListOfBooks = students.join(books, books.col("student_id") === students.col("id"))
      .select("firstName", "name")
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toList
      .groupBy(_._1)
      .map{ case (k, v) => (k, v.map(_._2)) }

    for ((k,v) <- studentsWithListOfBooks) println(k + " - " + v)
//    Vova - List(Harry Poter 2, Harry Poter 7_1)
//    Serhii - List(Harry Poter 1, Harry Poter 6)
//    Andrii - List(Harry Poter 3, Harry Poter 7_2)
//    Taras - List(Harry Poter 4)
//    Nazar - List(Harry Poter 5)

    books.createOrReplaceTempView("books")
    spark.sql("SELECT * FROM books b WHERE b.name LIKE '%Harry%7%'").show()
//    +---+---------------+----------+
//    | id|           name|student_id|
//    +---+---------------+----------+
//    |  8|Harry Poter 7_1|         3|
//    |  9|Harry Poter 7_2|         4|
//    +---+---------------+----------+

    val ageSum = MySum.toColumn.name("age_sum")
    val result = students.select(ageSum).show()

//    +-------+
//    |age_sum|
//    +-------+
//    |  114.0|
//    +-------+
  }
}
