package cn.bithachi.demo.pojo;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @author:erfeng_v
 * @create: 2022-11-26 23:21
 * @Description: 奖金与排名
 */
@Data
public class ScoreAward implements Serializable {

    private String sale_amount;

    private String rank;

    private String store_code;

    private BigDecimal award;

}
