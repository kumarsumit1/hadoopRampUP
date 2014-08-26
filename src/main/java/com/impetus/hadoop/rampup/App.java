package com.impetus.hadoop.rampup;

/**
 * Hello world!
 * 
 */
public class App {
  public static void main(String[] args) {

    String test = "196  242 3 881250949";
    String[] tes = test.split("\\s+");
    System.out.println(tes.length);
    System.out.println(tes);
    System.out.println(tes[1]);
    System.out.println(tes[2]);
    System.out.println(tes[3]);
    // System.out.println(tes[19]);
  }
}
