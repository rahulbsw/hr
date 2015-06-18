package com.ipass.firefly.hotel.exception;

/**
 * Created by rjain on 7/22/2014.
 */
public class IPassException extends  RuntimeException {

    private static final long serialVersionUID = 1L;

    public IPassException(String msg) {
        super(msg);
    }

    public IPassException(String msg, Throwable th) {
        super(msg , th);
    }



}
