package com.dajiangtai.dbus.model;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/3/7 下午2:27
 * 订单
 */


import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
@Data
@ToString
public class Orders implements Serializable {
    private Integer orderId;

    private String orderNo;

    private Integer userId;

    private Integer goodId;

    private BigDecimal goodsMoney;

    private BigDecimal realTotalMoney;

    private Integer payFrom;

    private String province;

    private Timestamp createTime;
}
