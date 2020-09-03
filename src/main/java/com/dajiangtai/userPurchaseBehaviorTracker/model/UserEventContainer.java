package com.dajiangtai.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/27 下午10:57
 */
@Data
@ToString
public class UserEventContainer {
    private String userId;
    private List<UserEvent> userEvents=new ArrayList<>();
}
