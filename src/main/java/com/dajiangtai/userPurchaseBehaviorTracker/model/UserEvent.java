package com.dajiangtai.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/27 下午8:48
 */
@Data
@ToString
public class UserEvent implements Serializable {

    private static final long serialVersionUID = -52934893611845880L;
    /**
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:11","data":{"productId":196}}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"ADD_TO_CART","eventTime":"2018-06-12 09:43:18","data":{"productId":126}}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:11","data":{"productId":126}}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"PURCHASE","eventTime":"2018-06-12 09:30:28","data":{"productId":196,"price":600.00,"amount":600.00}}
     */
    private String userId;
    private String channel;
    private String eventType;
    private long eventTime;
    private Product data;
}
