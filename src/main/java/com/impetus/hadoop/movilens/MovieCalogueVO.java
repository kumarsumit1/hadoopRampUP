package com.impetus.hadoop.movilens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MovieCalogueVO implements WritableComparable<MovieCalogueVO> {

  private IntWritable movieId;
  private Text movieTitle;
  private Text releaseDate;
  private Text videoReleaseDate;
  private Text IMDbURL;
  private Text unknown;
  private IntWritable action;
  private IntWritable adventure;
  private IntWritable animation;
  private IntWritable childrens;
  private IntWritable comedy;
  private IntWritable crime;
  private IntWritable documentary;
  private IntWritable drama;
  private IntWritable fantasy;
  private IntWritable filmNoir;
  private IntWritable horror;
  private IntWritable musical;
  private IntWritable mystery;
  private IntWritable romance;
  private IntWritable sciFi;
  private IntWritable thriller;
  private IntWritable war;
  private IntWritable western;

  public MovieCalogueVO() {
    // super();
    set(new IntWritable(), new Text(), new Text(), new Text(), new Text(),
        new Text(), new IntWritable(), new IntWritable(), new IntWritable(),
        new IntWritable(), new IntWritable(), new IntWritable(),
        new IntWritable(), new IntWritable(), new IntWritable(),
        new IntWritable(), new IntWritable(), new IntWritable(),
        new IntWritable(), new IntWritable(), new IntWritable(),
        new IntWritable(), new IntWritable(), new IntWritable());
  }

  public MovieCalogueVO(IntWritable movieId, Text movieTitle, Text releaseDate,
      Text videoReleaseDate, Text iMDbURL, Text unknown, IntWritable action,
      IntWritable adventure, IntWritable animation, IntWritable childrens,
      IntWritable comedy, IntWritable crime, IntWritable documentary,
      IntWritable drama, IntWritable fantasy, IntWritable filmNoir,
      IntWritable horror, IntWritable musical, IntWritable mystery,
      IntWritable romance, IntWritable sciFi, IntWritable thriller,
      IntWritable war, IntWritable western) {
    // super();
    set(movieId, movieTitle, releaseDate, videoReleaseDate, iMDbURL, unknown,
        action, adventure, animation, childrens, comedy, crime, documentary,
        drama, fantasy, filmNoir, horror, musical, mystery, romance, sciFi,
        thriller, war, western);
  }

  public void set(IntWritable movieId, Text movieTitle, Text releaseDate,
      Text videoReleaseDate, Text iMDbURL, Text unknown, IntWritable action,
      IntWritable adventure, IntWritable animation, IntWritable childrens,
      IntWritable comedy, IntWritable crime, IntWritable documentary,
      IntWritable drama, IntWritable fantasy, IntWritable filmNoir,
      IntWritable horror, IntWritable musical, IntWritable mystery,
      IntWritable romance, IntWritable sciFi, IntWritable thriller,
      IntWritable war, IntWritable western) {
    this.movieId = movieId;
    this.movieTitle = movieTitle;
    this.releaseDate = releaseDate;
    this.videoReleaseDate = videoReleaseDate;
    IMDbURL = iMDbURL;
    this.unknown = unknown;
    this.action = action;
    this.adventure = adventure;
    this.animation = animation;
    this.childrens = childrens;
    this.comedy = comedy;
    this.crime = crime;
    this.documentary = documentary;
    this.drama = drama;
    this.fantasy = fantasy;
    this.filmNoir = filmNoir;
    this.horror = horror;
    this.musical = musical;
    this.mystery = mystery;
    this.romance = romance;
    this.sciFi = sciFi;
    this.thriller = thriller;
    this.war = war;
    this.western = western;
  }

  public IntWritable getMovieId() {
    return movieId;
  }

  public void setMovieId(IntWritable movieId) {
    this.movieId = movieId;
  }

  public Text getMovieTitle() {
    return movieTitle;
  }

  public void setMovieTitle(Text movieTitle) {
    this.movieTitle = movieTitle;
  }

  public Text getReleaseDate() {
    return releaseDate;
  }

  public void setReleaseDate(Text releaseDate) {
    this.releaseDate = releaseDate;
  }

  public Text getVideoReleaseDate() {
    return videoReleaseDate;
  }

  public void setVideoReleaseDate(Text videoReleaseDate) {
    this.videoReleaseDate = videoReleaseDate;
  }

  public Text getIMDbURL() {
    return IMDbURL;
  }

  public void setIMDbURL(Text iMDbURL) {
    IMDbURL = iMDbURL;
  }

  public Text getUnknown() {
    return unknown;
  }

  public void setUnknown(Text unknown) {
    this.unknown = unknown;
  }

  public IntWritable getAction() {
    return action;
  }

  public void setAction(IntWritable action) {
    this.action = action;
  }

  public IntWritable getAdventure() {
    return adventure;
  }

  public void setAdventure(IntWritable adventure) {
    this.adventure = adventure;
  }

  public IntWritable getAnimation() {
    return animation;
  }

  public void setAnimation(IntWritable animation) {
    this.animation = animation;
  }

  public IntWritable getChildrens() {
    return childrens;
  }

  public void setChildrens(IntWritable childrens) {
    this.childrens = childrens;
  }

  public IntWritable getComedy() {
    return comedy;
  }

  public void setComedy(IntWritable comedy) {
    this.comedy = comedy;
  }

  public IntWritable getCrime() {
    return crime;
  }

  public void setCrime(IntWritable crime) {
    this.crime = crime;
  }

  public IntWritable getDocumentary() {
    return documentary;
  }

  public void setDocumentary(IntWritable documentary) {
    this.documentary = documentary;
  }

  public IntWritable getDrama() {
    return drama;
  }

  public void setDrama(IntWritable drama) {
    this.drama = drama;
  }

  public IntWritable getFantasy() {
    return fantasy;
  }

  public void setFantasy(IntWritable fantasy) {
    this.fantasy = fantasy;
  }

  public IntWritable getFilmNoir() {
    return filmNoir;
  }

  public void setFilmNoir(IntWritable filmNoir) {
    this.filmNoir = filmNoir;
  }

  public IntWritable getHorror() {
    return horror;
  }

  public void setHorror(IntWritable horror) {
    this.horror = horror;
  }

  public IntWritable getMusical() {
    return musical;
  }

  public void setMusical(IntWritable musical) {
    this.musical = musical;
  }

  public IntWritable getMystery() {
    return mystery;
  }

  public void setMystery(IntWritable mystery) {
    this.mystery = mystery;
  }

  public IntWritable getRomance() {
    return romance;
  }

  public void setRomance(IntWritable romance) {
    this.romance = romance;
  }

  public IntWritable getSciFi() {
    return sciFi;
  }

  public void setSciFi(IntWritable sciFi) {
    this.sciFi = sciFi;
  }

  public IntWritable getThriller() {
    return thriller;
  }

  public void setThriller(IntWritable thriller) {
    this.thriller = thriller;
  }

  public IntWritable getWar() {
    return war;
  }

  public void setWar(IntWritable war) {
    this.war = war;
  }

  public IntWritable getWestern() {
    return western;
  }

  public void setWestern(IntWritable western) {
    this.western = western;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    movieId.readFields(in);
    movieTitle.readFields(in);
    releaseDate.readFields(in);
    videoReleaseDate.readFields(in);
    IMDbURL.readFields(in);
    unknown.readFields(in);
    action.readFields(in);
    adventure.readFields(in);
    animation.readFields(in);
    childrens.readFields(in);
    comedy.readFields(in);
    crime.readFields(in);
    documentary.readFields(in);
    drama.readFields(in);
    fantasy.readFields(in);
    filmNoir.readFields(in);
    horror.readFields(in);
    musical.readFields(in);
    mystery.readFields(in);
    romance.readFields(in);
    sciFi.readFields(in);
    thriller.readFields(in);
    war.readFields(in);
    western.readFields(in);

  }

  @Override
  public void write(DataOutput out) throws IOException {
    movieId.write(out);
    movieTitle.write(out);
    releaseDate.write(out);
    videoReleaseDate.write(out);
    IMDbURL.write(out);
    unknown.write(out);
    action.write(out);
    adventure.write(out);
    animation.write(out);
    childrens.write(out);
    comedy.write(out);
    crime.write(out);
    documentary.write(out);
    drama.write(out);
    fantasy.write(out);
    filmNoir.write(out);
    horror.write(out);
    musical.write(out);
    mystery.write(out);
    romance.write(out);
    sciFi.write(out);
    thriller.write(out);
    war.write(out);
    western.write(out);

  }

  @Override
  public int compareTo(MovieCalogueVO o) {
    if (movieId.compareTo(o.movieId) == 0) {
      return (movieTitle.compareTo(o.movieTitle));
    } else
      return (movieId.compareTo(o.movieId));
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MovieCalogueVO) {
      MovieCalogueVO other = (MovieCalogueVO) o;
      return movieId.equals(other.movieId)
          && movieTitle.equals(other.movieTitle);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return movieId.hashCode();
  }

}
