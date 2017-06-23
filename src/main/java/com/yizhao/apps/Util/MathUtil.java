package com.yizhao.apps.Util;

import java.util.Arrays;

/**
 * Created by yzhao on 6/6/17.
 */
public class MathUtil {
    public static void main(String[] args){
        char[] digits1 = {'0','1'};
        char[] digits2 = {'0','9'};
        char[] digits3 = {'1','0'};
        System.out.println(Arrays.toString(plusOne(digits1))); // [0, 2]
        System.out.println(Arrays.toString(plusOne(digits2))); // [1, 0]
        System.out.println(Arrays.toString(plusOne(digits3))); // [1, 1]
        System.out.println(plusOne("2017-01".toCharArray())); // 2017-02
        System.out.println(plusOne("2017-12".toCharArray())); // 2017-13
    }

    public static char[] plusOne(char[] digits){
        int n = digits.length;
        for(int i = n - 1; i >= 0; i--){
            if(digits[i] < '9'){
                digits[i]++;
                char[] res = new char[n];
                for(int j = 0; j < n; j++){
                    res[j] = digits[j];
                }
                return res;
            }else{
                digits[i] = '0';
            }
        }

        char[] res = new char[n + 1];
        res[0] = '1';
        return res;
    }

}
