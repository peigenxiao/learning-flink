package com.dajiangtai.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/27 下午8:51
 */
@Data
@ToString
public class Product {
    private Integer productId;
    private double price;
    private Integer amount;
}
