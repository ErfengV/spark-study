package cn.bithachi.demo.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Student {

    private String name;
    private long age;
    private String sex;
}
