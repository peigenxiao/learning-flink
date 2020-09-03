package com.dajiangtai.dbus.enums;

import lombok.Getter;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/3/6 下午10:17
 */
@Getter
public enum FlowStatusEnum implements CodeEnum {
    /**
     * 初始状态(新添加)
     */
    FLOWSTATUS_INIT(0, "初始状态"),
    /**
     * 就绪状态
     */
    FLOWSTATUS_READY(1, "就绪状态"),
    /**
     * 运行状态
     */
    FLOWSTATUS_RUNNING(2, "运行状态");

    private Integer code;

    private String message;

    FlowStatusEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

}
