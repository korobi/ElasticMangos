package io.korobi.mongotoelastic.logging;

import com.google.inject.MembersInjector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;

public class Log4JMembersInjector<T> implements MembersInjector<T> {
    private final Field field;
    private final Logger logger;

    Log4JMembersInjector(Field field) {
        this.field = field;
        this.logger = LogManager.getLogger(field.getDeclaringClass());
        field.setAccessible(true);
    }

    public void injectMembers(T t) {
        try {
            this.field.set(t, logger);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}