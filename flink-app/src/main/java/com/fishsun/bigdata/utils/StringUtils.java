package com.fishsun.bigdata.utils;

public class StringUtils {
    /**
     * a given string is valid for number format or not
     * @param str
     * @return
     */
    public static boolean isValidIntegerWithRegex(String str) {
        if (str == null || str.trim().isEmpty()) {
            return false;
        }

        str = str.trim();

        // 正则表达式：可选的正负号，后接一个或多个数字
        String regex = "^[+-]?\\d+$";
        return str.matches(regex);
    }
}
