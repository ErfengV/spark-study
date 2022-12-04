package cn.bithachi.demo.pojo;


import cn.bithachi.demo.util.TabName;
import cn.bithachi.demo.util.Constant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@TabName(table = "student",condition = "where stat_date = "+ Constant.MODEL)
public class Student {

    private String name;
    private long age;
    private String sex;
}
