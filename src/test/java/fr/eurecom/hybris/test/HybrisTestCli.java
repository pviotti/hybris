package fr.eurecom.hybris.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.mds.Metadata;

public class HybrisTestCli implements Runnable {

    private final Hybris hybris;
    private static String HELP_STRING = "Usage:\n" +
            "\th - help\n" +
            "\tq - quit\n" +
            "\tw [key] [value] - write\n" +
            "\tr [key] - read\n" +
            "\td [key] - delete\n" +
            "\tl - list\n" +
            "\tla - list all\n" +
            "\tec - empty containers";

    public HybrisTestCli() throws HybrisException {
        this.hybris = new Hybris("hybris.properties");
    }

    @Override
    public void run() {
        System.out.println("Hybris test console\n===================");
        System.out.println("Type 'h' for help.");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        boolean quit = false;
        String line;
        while(!quit)
            try {
                System.out.print(">");
                line = br.readLine().trim().toLowerCase();

                if (line.equals("q"))
                    quit = true;
                else if (line.equals("h"))
                    System.out.println(HELP_STRING);
                else if (line.startsWith("w")) {
                    String key = line.split(" ")[1];
                    byte[] value = line.split(" ")[2].getBytes();
                    this.write(key, value);
                } else if (line.startsWith("r")) {
                    String key = line.split(" ")[1];
                    byte[] value = this.read(key);
                    if (value == null)
                        System.out.println("No value was found.");
                    else
                        System.out.println("Value retrieved: " + new String(value));
                } else if (line.startsWith("d")) {
                    String key = line.split(" ")[1];
                    this.delete(key);
                } else if (line.equals("l")) {
                    List<String> list = this.list();
                    for (String key : list)
                        System.out.println("\t - " + key);
                } else if (line.equals("la")) {
                    Map<String, Metadata> map = this.getAllMetadata();
                    for(String key : map.keySet())
                        System.out.println("\t - " + key + ": " + map.get(key));
                } else if (line.equals("ec"))
                    this.emptyContainers();
                else
                    System.out.println("* Unknown command.");

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("* Unknown command.");
            }

        /*try {
            this.hybris.gc();
        } catch (HybrisException e) { e.printStackTrace(); }*/

        this.hybris.shutdown();
    }

    private void write(String key, byte[] value) {
        try {
            this.hybris.write(key, value);
        } catch (HybrisException e) {
            e.printStackTrace();
        }
    }

    private byte[] read(String key) {
        try {
            return this.hybris.read(key);
        } catch (HybrisException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void delete(String key) {
        try {
            this.hybris.delete(key);
        } catch (HybrisException e) {
            e.printStackTrace();
        }
    }

    private List<String> list() {
        try {
            return this.hybris.list();
        } catch (HybrisException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Map<String, Metadata> getAllMetadata() {
        try {
            return this.hybris.getAllMetadata();
        } catch (HybrisException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void emptyContainers() {
        try {
            this.hybris._emptyContainers();
        } catch (HybrisException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws HybrisException {
        HybrisTestCli htc = new HybrisTestCli();
        htc.run();
    }
}