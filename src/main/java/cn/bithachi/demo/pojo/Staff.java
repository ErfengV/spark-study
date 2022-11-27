package cn.bithachi.demo.pojo;

import lombok.Data;

import java.io.Serializable;

/**
 * @author: erfeng_v
 * @create: 2022-11-26 22:55
 * @Description:
 */
@Data
public class Staff implements Serializable {

    private  String userId;

    private  String store_code;

    private  String score;

    private  String class_name;
}
