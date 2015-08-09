package io.korobi.mongotoelastic.logging;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;

/**
 * https://github.com/google/guice/wiki/CustomInjections
 */
public class LoggingModule extends AbstractModule {

    @Override
    protected void configure() {
        bindListener(Matchers.any(), new Log4JTypeListener());
    }
}
