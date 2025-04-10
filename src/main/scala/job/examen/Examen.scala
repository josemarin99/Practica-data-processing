package job.examen

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

object examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas */
  def ejercicio1(spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Crear un DataFrame con datos de estudiantes (nombre, edad, calificación)
    val estudiantes = Seq(
      ("Pedro", 19, 7.5),
      ("Marta", 21, 8.3),
      ("Jose", 23, 7.4),
      ("Antonio", 24, 9.0),
      ("Fede", 22, 6.5),
      ("Alba", 19, 9.8),
      ("Lucas", 20, 7.5),
      ("Bea", 21, 8.3),
      ("Juan", 22, 6.7),
      ("Carmen", 20, 8.2),
      ("Silvia", 23, 6.5),
      ("Maria", 21, 8.1),
      ("Karim", 24, 7.0),
      ("Celia", 18, 9.0),
      ("Marcos", 23, 7.9)
    ).toDF("nombre", "edad", "calificacion")

    // Mostrar el esquema del DataFrame
    estudiantes.printSchema()

    //Mostrar la tabla completa de estudiantes
    estudiantes.show()

    // Filtrar estudiantes con calificación mayor a 8
    val estudiantesFiltrados = estudiantes.filter(col("calificacion") > 8)

    // Seleccionar nombres y calificaciones, ordenados de forma descendente
    val resultado = estudiantesFiltrados.select("nombre", "calificacion").orderBy(col("calificacion").desc)

    resultado
  }

  /** Ejercicio 2: UDF (User Defined Function) */
  def ejercicio2(spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Crear un DataFrame con una lista de números
    val numerosDF = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("numero")

    // Definir la UDF para verificar si un número es par o impar
    val esPar = udf((numero: Int) => if (numero % 2 == 0) "Par" else "Impar")

    // Aplicar la UDF para determinar si cada número es par o impar
    val resultado = numerosDF.withColumn("par_o_impar", esPar($"numero"))

    // Mostrar el resultado
    resultado.show()

    resultado
  }

  /** Ejercicio 3: Joins y agregaciones */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    // Realizar el join entre los dos DataFrames usando el campo "id_estudiante" y "id"
    val estudiantesConCalificaciones = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .select("id", "nombre", "calificacion")

    // Calcular el promedio de calificaciones por estudiante
    val resultado = estudiantesConCalificaciones
      .groupBy("id", "nombre")  // Agrupar por id y nombre de estudiante
      .agg(avg("calificacion").alias("promedio_calificacion"))  // Calcular el promedio de calificación

    // Devolver el resultado con el promedio
    resultado
  }

  /** Ejercicio 4: Uso de RDDs */
  def ejercicio4(palabras: List[String])(spark: SparkSession): RDD[(String, Int)] = {
    // Convertir la lista de palabras en un RDD
    val palabrasRDD = spark.sparkContext.parallelize(palabras)

    // Contar las ocurrencias de cada palabra utilizando 'map' y 'reduceByKey'
    val conteoPalabrasRDD = palabrasRDD
      .map(palabra => (palabra, 1))  // Crear pares clave-valor (palabra, 1)
      .reduceByKey(_ + _)            // Sumar las ocurrencias de la misma palabra

    // Mostrar el resultado en la consola
    conteoPalabrasRDD.collect().foreach { case (palabra, conteo) =>
      println(s"Palabra: $palabra, Conteo: $conteo")
    }

    // Convertir el RDD a DataFrame para mostrarlo como una tabla
    import spark.implicits._
    val conteoPalabrasDF = conteoPalabrasRDD.toDF("Palabra", "Conteo")

    // Mostrar el DataFrame resultante
    conteoPalabrasDF.show()

    conteoPalabrasRDD
  }

  /** Ejercicio 5: Procesamiento de archivos */
  def ejercicio5(spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Cargar el archivo CSV
    val ventasDF = spark.read
      .option("header", "true") // Indica que la primera fila es de cabecera
      .option("inferSchema", "true") // Infiera el tipo de datos de las columnas
      .csv("C:/Users/jmari/OneDrive/Escritorio/csv/ventas.csv") // Ruta de tu archivo CSV

    // Mostrar el esquema y los primeros registros para verificar los datos cargados
    ventasDF.printSchema()
    ventasDF.show()

    // Calcular el ingreso total por producto
    val ventasConIngreso = ventasDF
      .withColumn("ingreso_total", $"cantidad" * $"precio_unitario") // Crear la columna de ingreso total

    // Agrupar por id_producto y calcular el total de ingresos por producto
    val resultado = ventasConIngreso
      .groupBy("id_producto")
      .agg(sum("ingreso_total").alias("ingreso_total_producto")) // Sumar el ingreso total por producto

    // Mostrar el resultado
    resultado.show()

    resultado
  }
}
