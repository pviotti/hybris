package fr.eurecom.hybris.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import eu.vandertil.jerasure.jni.Jerasure;
import eu.vandertil.jerasure.jni.ReedSolomon;

/**
 * Toy class for testing Jerasure JNI bindings.
 * @author P.Viotti
 */
public class JerasureTest {
    
    public static void load() {
        /* must set 
         * LD_LIBRARY_PATH=/path/to/jerasure/native/libs/
         * or install both libraries (jerasure and jerasure.jni)
         * into standard directories.
         * 
         * In Eclipse: Run Configuration.. -> Environment tab
         * 
         * NB: does not work with the -Djava.library.path
         * because we need both libraries; 
         * see http://javaagile.blogspot.fr/2014/04/jni-and-ldlibrarypath.html
         */
        
        System.loadLibrary("Jerasure.jni");
        System.out.println("Correctly loaded libJerasure.jni");
    }
    
    public static byte[] addPaddingIfNeeded(byte[] data, int blockSize) {
        if (data.length < blockSize) {
            data = Arrays.copyOf(data, blockSize);
        }
        return data;
    }
    
    public static void main(String[] args) {
        JerasureTest.load();
        
        String fileName = "origin.dat";
        int packetSize = 1024;
        int w = 8;
        int k = 2;
        int m = 2;
        
        // Encoding
        System.out.println("Encoding...");
        Path path = Paths.get(fileName);
        byte[] data = new byte[0];
        try {
            data = Files.readAllBytes(path);
        } catch (Exception e) {
            System.out.println("* Error trying to read file " + fileName);
            System.exit(-1);
        }
        
        // int data size in Java = 4
        int size = data.length;
        int newSize = size;
        if (size % (k * w * packetSize * 4) != 0) {
            while (newSize % ( k * w * packetSize * 4) != 0) 
                newSize++;
        }
        
        int blockSize = newSize / k;
        data = addPaddingIfNeeded(data, newSize);
        
        byte[][] dataBlocks = new byte[k][blockSize];
        for (int i = 0; i < k; i++) {
            dataBlocks[i] = new byte[blockSize];
            dataBlocks[i] = Arrays.copyOfRange(data, i*blockSize, i*blockSize + blockSize);
        }
        
        byte[][] codingBlocks = new byte[m][blockSize];
        for (int i = 0; i < m; i++)
            codingBlocks[i] = new byte[blockSize];

        int[] matrix = ReedSolomon.reed_sol_vandermonde_coding_matrix(k, m, w);
        Jerasure.jerasure_matrix_encode(k, m, w, matrix, dataBlocks, codingBlocks, blockSize);
        
        for (int i = 0; i < k; i++)
            try {
                Files.write(Paths.get("coded_k" + i + ".dat"), dataBlocks[i], StandardOpenOption.CREATE);
            } catch (IOException e) {
                System.out.println("* Error trying to write file " + "coded_k" + i + ".dat");
                System.exit(-1);
            }
        
        for (int i = 0; i < m; i++)
            try {
                Files.write(Paths.get("coded_m" + i + ".dat"), codingBlocks[i], StandardOpenOption.CREATE);
            } catch (IOException e) {
                System.out.println("* Error trying to write file " + "coded_m" + i + ".dat");
                System.exit(-1);
            }
        
        
        // Decode
        try {
            System.out.println("Press a key to decode...");
            System.in.read();
        } catch (IOException e1) { e1.printStackTrace(); }
        
        System.out.println("Decoding...");
        byte[][] decDataBlocks = new byte[k][blockSize];
        for (int i = 0; i < k; i++)
            decDataBlocks[i] = new byte[blockSize];        
        byte[][] decCodingBlocks = new byte[m][blockSize];
        for (int i = 0; i < m; i++)
            decCodingBlocks[i] = new byte[blockSize];
        
        int[] erasures = new int[k+m];
        int idxErasures = 0;
        
        for (int i = 0; i < k; i++)
            try {
                decDataBlocks[i] = Files.readAllBytes(Paths.get("coded_k" + i + ".dat"));
            } catch (Exception e) {
                System.out.println("* Erasure: " + "coded_k" + i + ".dat");
                erasures[idxErasures] = i;
                idxErasures++;
            }
        
        for (int i = 0; i < m; i++)
            try {
                decCodingBlocks[i] = Files.readAllBytes(Paths.get("coded_m" + i + ".dat"));
            } catch (Exception e) {
                System.out.println("* Erasure: " + "coded_m" + i + ".dat");
                erasures[idxErasures] = k + i;
                idxErasures++;
            }
        
        erasures[idxErasures] = -1;
        boolean res = Jerasure.jerasure_matrix_decode(k, m, w, matrix, true, erasures, decDataBlocks, decCodingBlocks, blockSize);
        
        if (!res) {
            System.out.println("* Error while decoding!");
            System.exit(-1);
        }
        
        try (FileOutputStream fos = new FileOutputStream("decoded.dat", false)) {
            int total = 0;
            for (int i = 0; i < k; i++) {
                if (total + blockSize <= size) {
                    fos.write(decDataBlocks[i]);
                    total+= blockSize;
                }
                else {
                    for (int j = 0; j < blockSize; j++) {
                        if (total < size) {
                            fos.write(decDataBlocks[i][j]);
                            total++;
                        } else 
                            break;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("* Error while writing decoded file!");
            System.exit(-1);
        }
     }
}
