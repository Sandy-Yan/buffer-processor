package com.github.andy.buffer.group.exception;

import java.util.concurrent.ExecutionException;

/**
 * Created by yanshanguang on 17/12/27.
 */
public class BufferException extends ExecutionException {

    public BufferException(String message) {
        super(message);
    }

    public BufferException(Throwable cause) {
        super(cause);
    }

    public BufferException(String message, Throwable cause) {
        super(message, cause);
    }

}
