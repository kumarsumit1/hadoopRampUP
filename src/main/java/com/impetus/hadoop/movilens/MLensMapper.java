package com.impetus.hadoop.movilens;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MLensMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private static HashMap<IntWritable, MovieCalogueVO> MovieMap =
      new HashMap<IntWritable, MovieCalogueVO>();

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    MovieCalogueVO mcatLog = new MovieCalogueVO();
    String[] words = value.toString().split("\\s+");
    // System.out.println(value.toString());
    if (words.length == 4) {
      System.out.println(words[1]);
      /*
       * System.out.printf(
       * "Words[0] - %s, Words[1] - %s, Words[2] - %s, length - %d", words[0],
       * words[1], words[2], words.length);
       */
      // mcatLog.setMovieId(new IntWritable(Integer.parseInt(words[0])));
      // mcatLog.setMovieTitle(new Text(words[1]));
      try {
        System.out.println("The size of the Moviemap is " + MovieMap.size());
        mcatLog = MovieMap.get(new IntWritable(Integer.parseInt(words[1])));
        System.out.println("the movie nae is" + mcatLog.getMovieTitle());
      } finally {
        if (mcatLog.getMovieId() == null
            || mcatLog.getMovieTitle().toString().isEmpty()
            || mcatLog.getMovieTitle().toString().equalsIgnoreCase("")) {
          System.out.println("data is null");
          mcatLog.setMovieId(new IntWritable(9999999));
          mcatLog.setMovieTitle(new Text("TestMOvie"));
        } else {

        }
      }
      context.write(mcatLog.getMovieTitle(),
          new IntWritable(Integer.parseInt(words[2])));
    } else {
      System.out.println("the row size is :" + words.length
          + " and its contents are:");
      for (String wrd : words) {
        System.out.println(wrd);
      }
    }
  }

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {

    Path[] cacheFilesLocal =
        DistributedCache.getLocalCacheFiles(context.getConfiguration());
    // URI[] cacheFiles =
    // DistributedCache.getCacheFiles(context.getConfiguration());

    // if (cacheFiles != null && cacheFiles.length > 0) {
    // for (URI cachedFile : cacheFiles) {
    /*
     * for (Path cachedFile : cacheFilesLocal) {
     * System.out.println("Inside setup(): " + cachedFile.toString());
     * System.out.println("Inside URI : " + cachedFile.toUri().toString()); //
     * loadDepartmentsHashMap(cacheFiles[0].toString(), context); String line;
     * BufferedReader cacheReader = new BufferedReader(new
     * FileReader(cachedFile.toString()));
     * System.out.println("after buffered reader"); try { while ((line =
     * cacheReader.readLine()) != null) { System.out.println("words added : " +
     * line.trim()); String[] words = line.toString().split("\\|");
     * MovieCalogueVO mcatLog = new MovieCalogueVO(); mcatLog.setMovieId(new
     * IntWritable(Integer.parseInt(words[0]))); mcatLog.setMovieTitle(new
     * Text(words[1])); MovieMap.put(mcatLog.getMovieId(), mcatLog);
     * 
     * } } finally { cacheReader.close(); }
     * 
     * }
     *//*
        * else { System.out.println("could not locate the file"); }
        */

    for (Path eachPath : cacheFilesLocal) {
      if (eachPath.getName().toString().trim().equals("u.item")) {
        loadDepartmentsHashMap(eachPath.toString(), context);
      }
    }

  }

  private void loadDepartmentsHashMap(String filePath, Context context)
      throws IOException {
    String strLineRead = "";
    BufferedReader brReader = null;
    try {
      brReader = new BufferedReader(new FileReader(filePath));

      while ((strLineRead = brReader.readLine()) != null) {
        String[] words = strLineRead.toString().split("\\|");
        MovieCalogueVO mcatLog = new MovieCalogueVO();
        mcatLog.setMovieId(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setMovieTitle(new Text(words[1]));
        mcatLog.setReleaseDate(new Text(words[1]));
        mcatLog.setVideoReleaseDate(new Text(words[1]));
        mcatLog.setIMDbURL(new Text(words[1]));
        mcatLog.setUnknown(new Text(words[1]));
        mcatLog.setAction(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setAdventure(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setAnimation(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setChildrens(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setComedy(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setCrime(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setDocumentary(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setDrama(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setFantasy(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setFilmNoir(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setHorror(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setMusical(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setMystery(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setRomance(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setSciFi(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setThriller(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setWar(new IntWritable(Integer.parseInt(words[0])));
        mcatLog.setWestern(new IntWritable(Integer.parseInt(words[0])));
        MovieMap.put(mcatLog.getMovieId(), mcatLog);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (brReader != null) {
        brReader.close();
      }
    }
  }

}
