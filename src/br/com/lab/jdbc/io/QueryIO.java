package br.com.lab.jdbc.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class QueryIO {
    public String arquivo;
    private BufferedReader buffer;


    public QueryIO(String caminho) {
        arquivo = caminho;
    }


    public String[] lerQuery() throws IOException {

        String linha;
        StringBuilder criaString = new StringBuilder();

        try {
            buffer = new BufferedReader(new FileReader(this.arquivo));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        while((linha = buffer.readLine()) != null) {
            criaString.append(linha);
        }

        String[] querys;
        querys = criaString.toString().split(";");
        return querys;
    }
}
