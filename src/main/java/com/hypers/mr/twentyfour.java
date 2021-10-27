package com.hypers.mr;

import java.util.Scanner;
/**
 * 24点游戏算法
 * @author Cshuang
 *
 */
public class twentyfour {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        double num = 0.0;
        while(in.hasNextInt()){
            int[] initNums = new int[4];
            int[] temp = new int[4];
            for (int i = 0; i < initNums.length; i++) {
                initNums[i] = in.nextInt();
            }
            System.out.println(check(initNums,temp, num));
        }
        in.close();
    }

    public static boolean check(int[] initNums,int[] temp,double num){
        for (int i = 0; i < initNums.length; i++) {
            if (temp[i]==0) {//之所以加上temp的限制，是避免元素本身和自己做运算
                temp[i] = 1;
                if (check(initNums, temp,num + initNums[i])
                        || check(initNums,temp ,num - initNums[i])
                        || check(initNums,temp ,num * initNums[i])
                        || check(initNums, temp,num / initNums[i])) {
                    return true;
                }
                temp[i] = 0;
            }
        }
        if(num==24)
            return true;
        else
            return false;
    }

}
