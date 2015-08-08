package io.korobi.opts;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class OptionsModule extends AbstractModule {

    private IOptions cmdLineOptions;

    public OptionsModule(IOptions cmdLineOptions) {
        this.cmdLineOptions = cmdLineOptions;
    }


    @Singleton
    @Provides
    public IOptions provideOptions() {
        return this.cmdLineOptions;
    }

    @Override
    protected void configure() {

    }
}
