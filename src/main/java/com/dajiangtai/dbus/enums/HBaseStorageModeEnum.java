package com.dajiangtai.dbus.enums;

import lombok.Getter;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/3/10 下午6:36
 */
@Getter
public enum HBaseStorageModeEnum implements CodeEnum{
    /**
     * STRING
     */
    STRING(0, "STRING"),
    /**
     * NATIVE
     */
    NATIVE(1, "NATIVE"),
    /**
     * PHOENIX
     */
    PHOENIX(2, "PHOENIX");

    private Integer code;

    private String message;

    HBaseStorageModeEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }
}
