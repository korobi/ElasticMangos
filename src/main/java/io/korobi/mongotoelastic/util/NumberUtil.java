package io.korobi.mongotoelastic.util;

public class NumberUtil {

    public static boolean isValueFractional(double number) {
        return Double.compare(Math.floor(number), number) != 0;
    }
}
