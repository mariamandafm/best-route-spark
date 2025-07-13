package org.example;

import java.io.*;
import java.util.Locale;
import java.util.Random;

public class DataGenerator {
    //private static final long FILE_SIZE = 200L * 1024 * 1024; // 200MB
    private static final long FILE_SIZE = 50L * 1024 * 1024; // 50MB
    //private static final long FILE_SIZE = 1000L * 1024 * 1024; // 1GB
    private static final String FILE_NAME = "coordenadas_50MB.csv";

    private static final String[] CIDADES = {
            "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Porto Alegre", "Curitiba",
            "Brasília", "Fortaleza", "Manaus", "Salvador", "Natal", "Palmas",
            "São Luís", "Campo Grande", "Apodi", "João Pessoa", "Parnamirim"
    };

    private static final int GROUP_SIZE = 50_000;

    public static void main(String[] args) {
        Locale.setDefault(Locale.US);

        Random random = new Random();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_NAME))) {
            long currentSize = 0;
            int count = 0;
            int currentGroup = 1;

            // Escreve cabeçalho CSV
            String header = "latitude;longitude;grupo\n";
            writer.write(header);
            currentSize += header.getBytes().length;

            while (currentSize < FILE_SIZE) {
                String record = generateCoordinates(random, currentGroup);
                writer.write(record);
                currentSize += record.getBytes().length;
                count++;

                if (count % GROUP_SIZE == 0) {
                    currentGroup++;
                }
            }

            System.out.println("Arquivo gerado: " + FILE_NAME);
            System.out.println("Tamanho final: " + (currentSize / (1024 * 1024)) + " MB");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String generateCoordinates(Random random, int groupId) {
        String cidade = CIDADES[random.nextInt(CIDADES.length)];
        double[] coords = getCityCoordinates(cidade);

        double latitude = coords[0] + (random.nextDouble() - 0.5) * 0.1;
        double longitude = coords[1] + (random.nextDouble() - 0.5) * 0.1;

        return String.format("%.6f;%.6f;%d\n", latitude, longitude, groupId);
    }

    private static double[] getCityCoordinates(String cidade) {
        switch (cidade) {
            case "São Paulo": return new double[]{-23.5505, -46.6333};
            case "Rio de Janeiro": return new double[]{-22.9068, -43.1729};
            case "Belo Horizonte": return new double[]{-19.9167, -43.9345};
            case "Porto Alegre": return new double[]{-30.0331, -51.2300};
            case "Curitiba": return new double[]{-25.4244, -49.2654};
            case "Brasília": return new double[]{-15.7939, -47.8828};
            case "Fortaleza": return new double[]{-3.7172, -38.5433};
            case "Manaus": return new double[]{-3.1190, -60.0217};
            case "Salvador": return new double[]{-12.9711, -38.5108};
            case "Natal": return new double[]{-5.7945, -35.2110};
            case "Palmas": return new double[]{-10.2491, -48.3243};
            case "São Luís": return new double[]{-2.5307, -44.3068};
            case "Campo Grande": return new double[]{-20.4697, -54.6201};
            case "Apodi": return new double[]{-5.6535, -37.7946};
            case "João Pessoa": return new double[]{-7.1153, -34.8610};
            case "Parnamirim": return new double[]{-5.9156, -35.2622};
            default: return new double[]{0, 0};
        }
    }
}
