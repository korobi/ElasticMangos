package io.korobi.utils;

public class NumberUtil {

    public static boolean isValueFractional(double number) {
        return Double.compare(Math.floor(number), number) != 0;
    }
}
