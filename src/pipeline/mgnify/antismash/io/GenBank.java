package pipeline.mgnify.antismash.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GenBank {

    File file;

    String accession = "";
    List<ProtoCluster> protoClusters = new ArrayList<>();
    public GenBank(File f) {
        this.file = f;
    }

    public void read() {


        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = "";
            boolean protoClusterStart = false;
            ProtoCluster pc = new ProtoCluster();
            while((line = br.readLine()) != null) {
                line = line.trim();
                if(line.startsWith("ACCESSION")) {
                    String[] lineSplit = line.split(" ");
                    accession = lineSplit[lineSplit.length - 1];
                } else if (line.startsWith("protocluster")) {
                    protoClusterStart = true;
                    pc = new ProtoCluster();
                } else if( protoClusterStart && line.startsWith("/category")) {
                    pc.category = line.split("=")[1].replaceAll("\"", "");
                } else if(protoClusterStart && line.startsWith("/contig_edge")) {
                    pc.contigEdge = line.split("=")[1].replaceAll("\"", "");
                } else if(protoClusterStart && line.startsWith("/product")) {
                    pc.product = line.split("=")[1].replaceAll("\"", "");
                } else if(protoClusterStart && line.startsWith("/protocluster_number")) {
                    pc.number = Integer.parseInt(line.split("=")[1].replaceAll("\"", ""));
                } else if(line.startsWith("proto_core")) {
                    protoClusterStart = false;
                    protoClusters.add(pc);
                    pc = new ProtoCluster();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class ProtoCluster {
        int number;
        String category;
        String contigEdge;
        String product;

        public int getNumber() {
            return number;
        }

        public String getCategory() {
            return category;
        }

        public String getContigEdge() {
            return contigEdge;
        }

        public String getProduct() {
            return product;
        }
    }

    public String getAccession() {
        return accession;
    }

    public List<ProtoCluster> getProtoClusters() {
        return protoClusters;
    }
}
