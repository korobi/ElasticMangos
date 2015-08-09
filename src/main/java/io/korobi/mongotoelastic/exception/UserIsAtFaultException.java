package io.korobi.mongotoelastic.exception;

public class UserIsAtFaultException extends RuntimeException {

    public UserIsAtFaultException(String message) {
        super(message);
    }
}
