package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SparkProcessCoordinates {

    public static void main(String[] args) throws IOException {
        String filePath = "C:\\Users\\amand\\code\\GeneticAlgorithm\\coordenadas_1000_50.txt";

        // Spark local
        SparkConf conf = new SparkConf()
                .setAppName("GeneticAlgorithmRouteOptimizer")
                .setMaster("local[*]") // Usa todos os núcleos disponíveis
                .set("spark.network.timeout", "600s")               // Aumenta o tempo de timeout
                .set("spark.executor.heartbeatInterval", "60s")
                .set("spark.executor.memory", "8g")
                .set("spark.driver.memory", "8g");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Leitura do arquivo como RDD de linhas
        JavaRDD<String> lines = sc.textFile(filePath);
        System.out.println(lines.getNumPartitions());

//        // Agrupa as linhas por grupo (separado por "-")
//        List<List<String>> groupedLines = groupLines(lines.collect());

//        System.out.println("---TRANSFORMANDO EM LISTA DE COORDENADAS----");

        // Transforma cada grupo em lista de coordenadas
//        JavaRDD<List<double[]>> coordinateGroupsRDD = sc.parallelize(groupedLines)
//                .map(SparkProcessCoordinates::parseCoordinates);
//
//        System.out.println("---PROCESSAMENTO FINALIZADO----");

//        // Aplica algoritmo genético em cada grupo
//        JavaPairRDD<List<double[]>, Long> indexedGroups = coordinateGroupsRDD.zipWithIndex();
//
//        JavaRDD<String> results = indexedGroups.map(tuple -> {
//            List<double[]> group = tuple._1;
//            long index = tuple._2;
//            List<double[]> bestRoute = GeneticAlgorithmPathFindingV2.geneticAlgorithm(group, 10, 10);
//            double distance = GeneticAlgorithmPathFindingV2.totalDistance(bestRoute);
//            return "Grupo " + index + ": " + distance;
//        });
//
//        // Salva no arquivo de saída
//        List<String> output = results.collect();
//        try (BufferedWriter writer = new BufferedWriter(new FileWriter("distancias.txt"))) {
//            for (String line : output) {
//                writer.write(line);
//                writer.newLine();
//            }
//        }

        sc.close();
    }

    private static List<List<String>> groupLines(List<String> lines) {
        List<List<String>> groups = new ArrayList<>();
        List<String> current = new ArrayList<>();
        for (String line : lines) {
            line = line.trim();
            if (line.equals("-")) {
                if (!current.isEmpty()) {
                    groups.add(new ArrayList<>(current));
                    current.clear();
                }
            } else {
                current.add(line);
            }
        }
        if (!current.isEmpty()) {
            groups.add(current);
        }
        return groups;
    }

    private static List<double[]> parseCoordinates(List<String> group) {
        return group.stream()
                .map(line -> {
                    String[] parts = line.split(";");
                    if (parts.length != 2) return null;
                    try {
                        double lat = Double.parseDouble(parts[0].replace(",", "."));
                        double lon = Double.parseDouble(parts[1].replace(",", "."));
                        return new double[]{lat, lon};
                    } catch (NumberFormatException e) {
                        System.err.println("Erro ao converter coordenadas: " + line);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}