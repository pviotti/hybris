package fr.eurecom.hybris.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;

public class HybrisTestCli implements Runnable {
    
    private static String HELP_STRING = "Usage:\n" +
                                        "\th - help\n" +
                                        "\tq - quit";
    
    private Hybris hybris;
    
    public HybrisTestCli() throws HybrisException {
        hybris = new Hybris();
    }
    
    @Override
    public void run() {
        System.out.println("Hybris test console\n===================");
        System.out.println("Type 'h' for help.");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        try {
            boolean quit = false;
            String line;
            while(!quit) {
                System.out.print(">");
                line = br.readLine().trim().toLowerCase();
                
                if (line.equalsIgnoreCase("q"))
                    quit = true;
                else if (line.equalsIgnoreCase("h"))
                    System.out.println(HELP_STRING);
                else
                    System.out.println("* Unknown command.");
                
                // TODO
            }
        } catch (IOException ioe) {
           ioe.printStackTrace();
           System.exit(1);
        }
    }
    

    public static void main(String[] args) throws HybrisException {
        HybrisTestCli htc = new HybrisTestCli();
        htc.run();
    }
}
