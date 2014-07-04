package fr.eurecom.hybris;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.vandertil.jerasure.jni.Jerasure;
import eu.vandertil.jerasure.jni.ReedSolomon;
import fr.eurecom.hybris.kvs.drivers.Kvs;

/**
 * Class in charge of performing erasure coding tasks.
 * @author P. Viotti
 */
public class EcManager {
    
    private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);
    
    private static String EC_LIB_NAME = "Jerasure.jni"; 
    private static int PACKET_SIZE = 8;    // 256 B minimum encoded block size
    private static int WORD_SIZE = 8;
    
    public enum ChunkState { KO, PENDING, OK };
    
    public class EcChunk {
        
        public byte[] data;
        public Kvs kvs;
        public ChunkState state;
        
        public EcChunk(byte[] data, Kvs kvs, ChunkState stored){
            this.data = data;
            this.kvs = kvs;
            this.state = stored;
        }
        
        public String toString() {
            return "[" + kvs + ", " + state + "]";
        }
    }
    
    public EcManager() {
        try {
            loadNativeLibrary();
        } catch(Throwable t) {
            logger.error("Could not load the erasure coding library.");
            throw t;
        }
    }
    
    private void loadNativeLibrary() {
        System.loadLibrary(EC_LIB_NAME);
        logger.debug("Correctly loaded libJerasure.jni");
    }
    
    private byte[] addPaddingIfNeeded(byte[] data, int blockSize) {
        if (data.length < blockSize) {
            data = Arrays.copyOf(data, blockSize);
        }
        return data;
    }
    
    private int[] getCodingMatrix(int k, int m) {
        return ReedSolomon.reed_sol_vandermonde_coding_matrix(k, m, WORD_SIZE);
    }
    
    private int getPaddedSize(int originalSize, int k) {
        int newSize = originalSize;
        if (originalSize % (k * WORD_SIZE * PACKET_SIZE * 4) != 0)
            while (newSize % ( k * WORD_SIZE * PACKET_SIZE * 4) != 0) 
                newSize++;
        return newSize;
    }
    
    public byte[][] encode(byte[] data, int k, int m) {
        
        int paddedSize = getPaddedSize(data.length, k);
        data = addPaddingIfNeeded(data, paddedSize);
        int blockSize = paddedSize / k;
        
        byte[][] dataBlocks = new byte[k][blockSize];
        for (int i = 0; i < k; i++) {
            dataBlocks[i] = new byte[blockSize];
            dataBlocks[i] = Arrays.copyOfRange(data, i*blockSize, i*blockSize + blockSize);
        }
        
        byte[][] codingBlocks = new byte[m][blockSize];
        for (int i = 0; i < m; i++)
            codingBlocks[i] = new byte[blockSize];

        int[] matrix = getCodingMatrix(k, m);
        Jerasure.jerasure_matrix_encode(k, m, WORD_SIZE, matrix, dataBlocks, codingBlocks, blockSize);
        
        byte[][] dataAndCoding = new byte[k+m][blockSize]; 
        for (int i=0; i<k; i++)
            dataAndCoding[i] = dataBlocks[i];
        for (int i=0; i<m; i++)
            dataAndCoding[k+i] = codingBlocks[i];
        
        return dataAndCoding;
    }
    
    public byte[] decode(byte[][] dataBlocks, byte[][] codingBlocks, int[] erasures, int k, int m, int originalSize) throws HybrisException {
        
        int paddedSize = getPaddedSize(originalSize, k);
        int blockSize = paddedSize / k;
        
        int[] matrix = getCodingMatrix(k, m);
        boolean res = Jerasure.jerasure_matrix_decode(k, m, WORD_SIZE, matrix, true, erasures, dataBlocks, codingBlocks, blockSize);
        
        if (!res) {
            logger.error("Error while decoding");
            throw new HybrisException("Error while decoding");
        }
        
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(originalSize)) {
        
            int total = 0;
            for (int i = 0; i < k; i++) {
                if (total + blockSize <= originalSize) {
                    bos.write(dataBlocks[i]);
                    total+= blockSize;
                }
                else {
                    for (int j = 0; j < blockSize; j++) {
                        if (total < originalSize) {
                            bos.write(dataBlocks[i][j]);
                            total++;
                        } else 
                            break;
                    }
                }
            }
            
            return bos.toByteArray();
        } catch (Exception e) {
            logger.error("I/O Error while writing decoded data to byte array");
            throw new HybrisException("I/O Error while writing decoded data to byte array");
        }
    }
}
