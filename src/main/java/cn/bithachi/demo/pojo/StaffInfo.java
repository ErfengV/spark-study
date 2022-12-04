package cn.bithachi.demo.pojo;

import lombok.Data;

import java.io.Serializable;

/**
 * @author:erfeng_v
 * @create: 2022-12-03 15:46
 * @Description: 人员信息
 */
@Data
public class StaffInfo implements Serializable {


    private String score;

    private String class_name;

    private String id;

    private String store_code;

    private String username;
}
