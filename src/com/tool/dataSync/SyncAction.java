package com.tool.dataSync;

import lombok.Getter;

/**
 * Created by peterchen on 30/07/2018.
 */
public enum SyncAction {

    DELETE(0), DEFAULT(1), INSERT(2), UPDATE(3);

    @Getter
    private int code;

    SyncAction(int code) {
        this.code = code;
    }
}
