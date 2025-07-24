package org.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SparkProcessCoordinates {

    public static void main(String[] args) throws IOException, InterruptedException {

        SparkSession spark = SparkSession.builder()
                .appName("GeneticRouteDataset")
                .master("local[*]")
                .getOrCreate();

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

        Encoder<BestRouteResult> resultEncoder = Encoders.bean(BestRouteResult.class);

        Dataset<BestRouteResult> resultDataset = grouped.map((MapFunction<Row, BestRouteResult>) row -> {
            String grupo = row.getAs("grupo").toString();
            List<Row> pontos = row.getList(row.fieldIndex("pontos"));

            List<double[]> coordenadas = new ArrayList<>();
            for (Row ponto : pontos) {
                double lat = ponto.getDouble(0);
                double lon = ponto.getDouble(1);
                coordenadas.add(new double[]{lat, lon});
            }

            List<double[]> bestRoute = GeneticAlgorithmPathFindingV2.geneticAlgorithm(coordenadas, 10, 10);
            double distancia = GeneticAlgorithmPathFindingV2.totalDistance(bestRoute);

            BestRouteResult result = new BestRouteResult();
            result.setGrupo(grupo);
            result.setDistancia(distancia);
            return result;
        }, resultEncoder);

        resultDataset.map((MapFunction<BestRouteResult, String>) BestRouteResult::toString, Encoders.STRING())
                .write()
                .text("saida/melhores_rotas");

        //Thread.sleep(1000000);
        spark.stop();
    }
}