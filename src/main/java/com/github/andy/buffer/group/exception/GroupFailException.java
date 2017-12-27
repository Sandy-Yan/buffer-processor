package com.github.andy.buffer.group.exception;

import java.util.concurrent.ExecutionException;

/**
 * 分组失败异常
 * Created by yanshanguang on 17/12/27.
 */
public class GroupFailException extends ExecutionException {

    public GroupFailException(String message) {
        super(message);
    }

    public GroupFailException(Throwable cause) {
        super(cause);
    }

    public GroupFailException(String message, Throwable cause) {
        super(message, cause);
    }

}
