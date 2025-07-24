package org.example;

public class BestRouteResult {
    private String grupo;
    private double distancia;

    public BestRouteResult() {}

    public String getGrupo() { return grupo; }
    public void setGrupo(String grupo) { this.grupo = grupo; }

    public double getDistancia() { return distancia; }
    public void setDistancia(double distancia) { this.distancia = distancia; }

    @Override
    public String toString() {
        return grupo + ";" + String.format("%.6f", distancia);
    }
}
