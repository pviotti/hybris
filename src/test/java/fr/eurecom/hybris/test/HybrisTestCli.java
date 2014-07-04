/**
 * Copyright (C) 2013 EURECOM (www.eurecom.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.eurecom.hybris.test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import fr.eurecom.hybris.GcManager;
import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.mds.Metadata;

public class HybrisTestCli implements Runnable {

    private final Hybris hybris;
    private static String HELP_STRING = "Usage:\n" +
            "\th - help\n" +
            "\tq - quit\n" +
            "\tw [key] [file name] - write\n" +
            "\tr [key] - read\n" +
            "\td [key] - delete\n" +
            "\tl - list\n" +
            "\tla - list all\n" +
            "\tec - empty containers\n" +
            "\tcf [name] [size in kB] - create a file";

    public HybrisTestCli() throws Exception {
        this.hybris = new Hybris("hybris.properties");
    }

    public void run() {
        System.out.println("Hybris test console\n===================");
        System.out.println("Type 'h' for help.");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        boolean quit = false;
        String line;
        while(!quit)
            try {
                System.out.print("> ");
                line = br.readLine().trim().toLowerCase();

                if (line.equals("q"))
                    quit = true;
                else if (line.equals("h"))
                    System.out.println(HELP_STRING);
                else if (line.startsWith("w")) {
                    String key = line.split(" ")[1];
                    String fileName = line.split(" ")[2];
                    Path path = Paths.get(fileName);
                    byte[] value;
                    try {
                        value = Files.readAllBytes(path);
                    } catch (Exception e) {
                        System.out.println("* Error trying to read file " + fileName);
                        continue;
                    }
                    this.write(key, value);
                } else if (line.startsWith("r")) {
                    String key = line.split(" ")[1];
                    byte[] value = this.read(key);
                    if (value == null)
                        System.out.println("No value was found.");
                    else
                        System.out.println("Value retrieved for key " + key + ",  " + value.length + " bytes.");
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
                } else if (line.startsWith("cf")) {
                    String fileName = line.split(" ")[1];
                    int fileSize = Integer.parseInt(line.split(" ")[2]);
                    this.createFile(fileName, fileSize);
                    System.out.println("Local file " + fileName + " of " + fileSize + " kB has been created.");
                } else if (line.equals("ec"))
                    this.emptyContainers();
                else
                    System.out.println("* Unknown command.");

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("* Unknown command.");
            }

        //        try {
        //            this.hybris.new GcManager().gc();
        //        } catch (HybrisException e) { e.printStackTrace(); }

        this.hybris.shutdown();
    }

    private void write(String key, byte[] value) {
        try {
            this.hybris.put(key, value);
        } catch (HybrisException e) {
            e.printStackTrace();
        }
    }

    private byte[] read(String key) {
        try {
            return this.hybris.get(key);
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
            new GcManager(hybris)._emptyContainers();
        } catch (HybrisException e) {
            e.printStackTrace();
        }
    }

    private void createFile(String name, int size) {
        File file = new File(name);
        if (file.isFile()) file.delete();
        byte[] array = new byte[size * 1024];
        Arrays.fill(array, (byte) 'x');
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file))) {
            bos.write(array, 0, size * 1024);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        new HybrisTestCli().run();
    }
}