package io.korobi.exceptions;

public class UserIsAtFaultException extends RuntimeException {

    public UserIsAtFaultException(String message) {
        super(message);
    }
}
