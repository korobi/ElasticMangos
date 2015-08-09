package io.korobi.mongotoelastic.logging;

import com.google.inject.TypeLiteral;
import com.google.inject.spi.ProvisionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;

public class Log4JTypeListener implements TypeListener, ProvisionListener {
    @Override
    public <T> void onProvision(ProvisionInvocation<T> provisionInvocation) {

    }

    @Override
    public <T> void hear(TypeLiteral<T> typeLiteral, TypeEncounter<T> typeEncounter) {
        System.out.println("Call!");
        Class<?> clazz = typeLiteral.getRawType();
        while (clazz != null) {
            System.out.println("For: " + typeLiteral.getType().getTypeName());
            for (Field field : clazz.getDeclaredFields()) {
                if (field.getType() == Logger.class && field.isAnnotationPresent(InjectLogger.class)) {
                    System.out.println("Found injectable field :D");
                    typeEncounter.register(new Log4JMembersInjector<T>(field));
                }
            }
            clazz = clazz.getSuperclass();
        }
    }
}
