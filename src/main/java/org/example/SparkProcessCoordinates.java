package org.example;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SparkProcessCoordinates {

    public static void main(String[] args) throws IOException {
        //System.setProperty("hadoop.home.dir", "C:\\hadoop");
        //String filePath = "C:\\Users\\amand\\code\\GeneticAlgorithm\\coordenadas_200MB.txt";

        SparkSession spark = SparkSession.builder()
                .appName("GeneticRouteDataset")
                .master("local[*]")
                .getOrCreate();

        //spark.sparkContext().hadoopConfiguration().set("hadoop.native.lib", "false");

        StructType schema = new StructType(new StructField[]{
                new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("grupo", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(schema)
                .csv("coordenadas_1GB.csv");


        df.show(5);
        df.printSchema();

        Dataset<Row> grouped = df.groupBy("grupo").agg(functions.collect_list(
                functions.struct("latitude", "longitude")).alias("pontos"));

        grouped.show(5);

        JavaRDD<Row> groupedRDD = grouped.toJavaRDD();

        JavaRDD<String> results = groupedRDD.map(row -> {
            String grupo = row.getAs("grupo").toString();

            // pontos é uma List<Row>
            List<Row> pontos = row.getList(1);

            List<double[]> coordenadas = new ArrayList<>();
            for (Row ponto : pontos) {
                double lat = ponto.getDouble(0);
                double lon = ponto.getDouble(1);
                coordenadas.add(new double[]{lat, lon});
            }

            // Executa algoritmo genético (supondo assinatura estática)
            List<double[]> bestRoute = GeneticAlgorithmPathFindingV2.geneticAlgorithm(coordenadas, 10, 10);
            double distance = GeneticAlgorithmPathFindingV2.totalDistance(bestRoute);

            // Formata resultado para salvar
            return String.format("%s;%.6f", grupo, distance);
        });

        results.saveAsTextFile("saida/melhores_rotas");
        spark.stop();
    }
}