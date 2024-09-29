package com.fishsun.bigdata;

import com.google.gson.Gson;

public class JSONUtils {
  private static final Gson gson = new Gson();

  public static String toJson(Object obj) {
    return gson.toJson(obj);
  }
}
