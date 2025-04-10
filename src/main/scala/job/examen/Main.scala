package job.examen

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    // Crear una SparkSession
    val spark = SparkSession.builder()
      .appName("Ejercicios Spark")
      .master("local[*]") // Usar todos los núcleos disponibles en el equipo
      .getOrCreate()

    // Llamar y mostrar los resultados de cada ejercicio
    mostrarEjercicio1(spark)
    mostrarEjercicio2(spark)
    mostrarEjercicio3(spark)
    mostrarEjercicio4(spark)  // Mostrar el Ejercicio 4
    mostrarEjercicio5(spark)  // Mostrar el Ejercicio 5

    // Cerrar la sesión de Spark
    spark.stop()
  }

  // Ejercicio 1: Función para el primer ejercicio
  def mostrarEjercicio1(spark: SparkSession): Unit = {
    val resultado1 = examen.ejercicio1(spark)
    println("Resultados del Ejercicio 1 (Estudiantes filtrados):")
    resultado1.show()  // Mostrar resultados del Ejercicio 1
  }

  // Ejercicio 2: Función para el segundo ejercicio
  def mostrarEjercicio2(spark: SparkSession): Unit = {
    val resultado2 = examen.ejercicio2(spark)
    println("Resultados del Ejercicio 2 (Números pares e impares):")
    resultado2.show()  // Mostrar resultados del Ejercicio 2
  }

  // Ejercicio 3: Función para el tercer ejercicio (Joins y agregaciones)
  def mostrarEjercicio3(spark: SparkSession): Unit = {
    import spark.implicits._

    // Crear DataFrame de estudiantes con los datos proporcionados
    val estudiantes = Seq(
      (1, "Pedro"),
      (2, "Marta"),
      (3, "Jose"),
      (4, "Antonio"),
      (5, "Fede"),
      (6, "Alba"),
      (7, "Lucas"),
      (8, "Bea"),
      (9, "Juan"),
      (10, "Carmen"),
      (11, "Silvia"),
      (12, "Maria"),
      (13, "Karim"),
      (14, "Celia"),
      (15, "Marcos"),
      (16, "Gara"),
      (17, "Cyntia"),
      (18, "Gema"),
      (19, "David"),
      (20, "Lidia")
    ).toDF("id", "nombre")

    // Crear DataFrame de calificaciones con los datos proporcionados
    val calificaciones = Seq(
      (1, "Química", 9.0), (1, "Física", 7.8),
      (2, "Química", 9.6), (2, "Física", 8.0),
      (3, "Química", 6.5), (3, "Física", 7.9),
      (4, "Química", 9.2), (4, "Física", 9.0),
      (5, "Química", 6.5), (5, "Física", 6.0),
      (6, "Química", 9.7), (6, "Física", 9.8),
      (7, "Química", 7.6), (7, "Física", 8.0),
      (8, "Química", 8.7), (8, "Física", 7.8),
      (9, "Química", 7.0), (9, "Física", 5.7),
      (10, "Química", 9.0), (10, "Física", 8.1),
      (11, "Química", 6.5), (11, "Física", 6.0),
      (12, "Química", 8.5), (12, "Física", 7.6),
      (13, "Química", 6.9), (13, "Física", 8.0),
      (14, "Química", 9.5), (14, "Física", 9.2),
      (15, "Química", 8.0), (15, "Física", 6.0),
      (16, "Química", 8.2), (16, "Física", 9.3),
      (17, "Química", 6.4), (17, "Física", 7.0),
      (18, "Química", 6.2), (18, "Física", 7.3),
      (19, "Química", 8.5), (19, "Física", 7.9),
      (20, "Química", 8.2), (20, "Física", 8.5)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    // Llamar al ejercicio 3 y mostrar los resultados
    val resultado3 = examen.ejercicio3(estudiantes, calificaciones)
    println("Resultados del Ejercicio 3 (Promedio de calificaciones por estudiante):")
    resultado3.show()  // Mostrar los resultados del ejercicio 3
  }

  // Ejercicio 4: Función para el cuarto ejercicio (Uso de RDDs)
  def mostrarEjercicio4(spark: SparkSession): Unit = {
    val palabras = List(
      "Lakers", "Bulls", "Celtics", "Warriors", "Lakers",
      "Heat", "Spurs", "Bulls", "Nets", "Bulls", "Bulls", "Knicks",
      "Mavericks", "Lakers", "Lakers", "Suns", "Clippers", "76ers", "Heat",
      "Lakers", "Rockets", "Nuggets", "Magic", "Brooklin", "Heat", "Suns"
    )

    val resultado4 = examen.ejercicio4(palabras)(spark)
    println("Resultados del Ejercicio 4 (Conteo de ocurrencias de palabras):")
    // Ya se muestra en consola dentro de la función `ejercicio4`
  }

  // Ejercicio 5: Función para el quinto ejercicio (Procesamiento de archivos)
  def mostrarEjercicio5(spark: SparkSession): Unit = {
    val resultado5 = examen.ejercicio5(spark)
    println("Resultados del Ejercicio 5 (Ingreso total por producto):")
    resultado5.show()  // Mostrar los resultados del ejercicio 5
  }
}
