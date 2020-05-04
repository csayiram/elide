/*
 * Copyright 2017, Oath Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.standalone.config;

import com.yahoo.elide.Elide;
import com.yahoo.elide.ElideSettings;
import com.yahoo.elide.async.models.AsyncQuery;
import com.yahoo.elide.async.models.AsyncQueryResult;
import com.yahoo.elide.async.service.AsyncCleanerService;
import com.yahoo.elide.async.service.AsyncExecutorService;
import com.yahoo.elide.async.service.AsyncQueryDAO;
import com.yahoo.elide.async.service.DefaultAsyncQueryDAO;
import com.yahoo.elide.contrib.swagger.SwaggerBuilder;
import com.yahoo.elide.core.DataStore;
import com.yahoo.elide.core.EntityDictionary;
import com.yahoo.elide.graphql.Entity;
import com.yahoo.elide.standalone.Util;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

import io.swagger.models.Info;
import io.swagger.models.Swagger;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

/**
 * Elide application resource configuration file.
 */
@Slf4j
public class ElideResourceConfig extends ResourceConfig {
    private final ElideStandaloneSettings settings;
    private final ServiceLocator injector;

    public static final String ELIDE_STANDALONE_SETTINGS_ATTR = "elideStandaloneSettings";

    private static MetricRegistry metricRegistry = null;
    private static HealthCheckRegistry healthCheckRegistry = null;

    /**
     * Constructor.
     *
     * @param injector Injection instance for application.
     * @throws Exception
     */
    @Inject
    public ElideResourceConfig(ServiceLocator injector, @Context ServletContext servletContext) throws Exception {
        this.injector = injector;

        settings = (ElideStandaloneSettings) servletContext.getAttribute(ELIDE_STANDALONE_SETTINGS_ATTR);

        if (settings.enableDynamicModelConfig()) {
            Util.initDynamicConfig(settings.getDynamicConfigPath());
        }

        // Bind things that should be injectable to the Settings class
        register(new AbstractBinder() {
            @Override
            protected void configure() {
                try {
                    bind(Util.combineModelEntities(Util.dynamicEntityCompiler,
                            settings.getModelPackageName(), settings.enableAsync(),
                            settings.enableDynamicModelConfig())).to(Set.class).named("elideAllModels");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    log.debug("error in resource config" + e.getMessage());
                }
            }
        });

        // Bind to injector
        register(new AbstractBinder() {
            @Override
            protected void configure() {
                ElideSettings elideSettings = null;
                try {
                    elideSettings = settings.getElideSettings(injector);
                } catch (ClassNotFoundException e) {
                    //Cannot throw ClassNotFoundException, still need to fail startup.
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                Elide elide = new Elide(elideSettings);

                // Bind elide instance for injection into endpoint
                bind(elide).to(Elide.class).named("elide");

                // Bind additional elements
                bind(elideSettings).to(ElideSettings.class);
                bind(elideSettings.getDictionary()).to(EntityDictionary.class);
                bind(elideSettings.getDataStore()).to(DataStore.class).named("elideDataStore");

                // Binding async service
                if (settings.enableAsync()) {
                    AsyncQueryDAO asyncQueryDao = settings.getAsyncQueryDAO();
                    if (asyncQueryDao == null) {
                        asyncQueryDao = new DefaultAsyncQueryDAO(elide, elide.getDataStore());
                    }
                    bind(asyncQueryDao).to(AsyncQueryDAO.class);

                    AsyncExecutorService.init(elide, settings.getAsyncThreadSize(),
                            settings.getAsyncMaxRunTimeMinutes(), asyncQueryDao);
                    bind(AsyncExecutorService.getInstance()).to(AsyncExecutorService.class);

                    // Binding async cleanup service
                    if (settings.enableAsyncCleanup()) {
                        AsyncCleanerService.init(elide, settings.getAsyncMaxRunTimeMinutes(),
                                settings.getAsyncQueryCleanupDays(), asyncQueryDao);
                        bind(AsyncCleanerService.getInstance()).to(AsyncCleanerService.class);
                    }
                }
            }
        });

        // Bind swaggers to given endpoint
        register(new org.glassfish.hk2.utilities.binding.AbstractBinder() {
            @SuppressWarnings("rawtypes")
            @Override
            protected void configure() {
                Map<String, Swagger> swaggerDocs = settings.enableSwagger();
                if (!swaggerDocs.isEmpty()) {
                    // Include the async models in swagger docs
                    if (settings.enableAsync()) {
                        EntityDictionary dictionary = new EntityDictionary(new HashMap<>());
                        dictionary.bindEntity(AsyncQuery.class);
                        dictionary.bindEntity(AsyncQueryResult.class);
                        Swagger swagger = setupSwagger(dictionary, settings.getJsonApiPathSpec(), "Async Service");
                        swaggerDocs.put("async", swagger);
                    }
                    // bind dynamic models
                    if (settings.enableDynamicModelConfig()) {
                        EntityDictionary dictionary = new EntityDictionary(new HashMap<>());
                        try {
                            for (Class entity : Util.dynamicEntityCompiler.findAnnotatedClasses(Entity.class)) {
                                dictionary.bindEntity(entity);
                            }
                        } catch (ClassNotFoundException e) {
                            log.error("error while binding class");
                        }

                        Swagger swagger = setupSwagger(dictionary, settings.getJsonApiPathSpec(),
                            "Dynamic models Service");
                        swaggerDocs.put("dynamic", swagger);
                    }

                    bind(swaggerDocs).named("swagger").to(new TypeLiteral<Map<String, Swagger>>() { });
                }
            }
        });

        registerFilters(settings.getFilters());

        additionalConfiguration(settings.getApplicationConfigurator());
    }

    /**
     * Init the supplemental resource config.
     */
    private void additionalConfiguration(Consumer<ResourceConfig> configurator) {
        // Inject into consumer if class is provided
        injector.inject(configurator);
        configurator.accept(this);
    }

    /**
     * Register provided JAX-RS filters.
     */
    private void registerFilters(List<Class<?>> filters) {
        filters.forEach(this::register);
    }

    public static MetricRegistry getMetricRegistry() {
        if (metricRegistry == null) {
            metricRegistry = new MetricRegistry();
        }

        return metricRegistry;
    }

    public static HealthCheckRegistry getHealthCheckRegistry() {
        if (healthCheckRegistry == null) {
            healthCheckRegistry = new HealthCheckRegistry();
        }

        return healthCheckRegistry;
    }

    private Swagger setupSwagger(EntityDictionary dictionary, String path, String title) {
        Info info = new Info().title(title).version("1.0");
        SwaggerBuilder builder = new SwaggerBuilder(dictionary, info);

        //Default value of getJsonApiPathSpec() ends with /* at the end. need to remove.
        String moduleBasePath = path.replaceAll("/\\*", "");

        Swagger swagger = builder.build().basePath(moduleBasePath);
        return swagger;
    }
}
