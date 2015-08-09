package io.korobi.mongotoelastic.logging;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.matcher.Matchers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Singleton;

/**
 * https://github.com/google/guice/wiki/CustomInjections
 */
public class LoggingModule extends AbstractModule {

    @Override
    protected void configure() {
        bindListener(Matchers.any(), new Log4JTypeListener());
    }

    @Provides
    @Singleton
    public Logger provideLogger() {
        return LogManager.getLogger();
    }
}
