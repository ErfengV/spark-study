package cn.bithachi.demo.rdd;

import cn.bithachi.demo.pojo.ScoreAward;

import java.util.Arrays;
import java.util.List;

/**
 * @author:erfeng_v
 * @create: 2022-11-27 00:35
 * @Description: 奖罚区间，》》》》》
 * 都需要对其先rank一遍，然后开始进行奖罚
 * 求出奖罚区间
 * 在对应的区间内的奖罚 得出最终的钱
 *
 *
 */
public class Test {

    public static void main(String[] args) {

//        List<Integer> integers = Arrays.asList(1, 1, 3, 3, 3, 6, 6, 6, 6, 6, 6, 6, 13, 14, 15, 16);
        List<Integer> integers = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 16);
        System.out.println(integers.size());
        double size = Math.floor(integers.size() * 0.3);
        System.out.println(size);
        //向下取整系数
        // 第一名： [ 1 - radio ]
        // 第二名： [ radio+1 - radio+radio ]
        // 第三名： [ radio+radio+1 - quota ]
        double first = Math.floor(size / 3);

        double second = first * 2;
        for (int i = 0; i < integers.size(); i++) {
            if(integers.get(i) <= first){
                System.out.println("奖励；10");
            }else if(integers.get(i) <= second){
                System.out.println("奖励；20");
            }else if(integers.get(i) <= size){
                System.out.println("奖励；30");
            }
        }

        System.out.println(Math.floor(21*0.3));
    }
}
