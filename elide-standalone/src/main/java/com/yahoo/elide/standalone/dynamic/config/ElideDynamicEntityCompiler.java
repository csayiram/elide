/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.standalone.dynamic.config;

import com.yahoo.elide.contrib.dynamicconfighelpers.model.ElideSecurityConfig;
import com.yahoo.elide.contrib.dynamicconfighelpers.model.ElideTableConfig;
import com.yahoo.elide.contrib.dynamicconfighelpers.parser.ElideConfigParser;
import com.yahoo.elide.contrib.dynamicconfighelpers.parser.handlebars.HandlebarsHydrator;

import com.google.common.collect.Sets;
import org.mdkt.compiler.InMemoryJavaCompiler;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Elide Dynamic entity compiler.
 */
@Slf4j
public class ElideDynamicEntityCompiler {

    @SuppressWarnings("rawtypes")
    public static Set<Class> bindClasses;
    public static final String PACKAGE_NAME = "com.yahoo.elide.contrib.dynamicconfig.model.";
    public static List<String> classNames = new ArrayList<String>();

    private static InMemoryJavaCompiler inMemoryJavaCompiler = InMemoryJavaCompiler.newInstance();
    private static ElideConfigParser elideConfigParser = new ElideConfigParser();
    private static Map<String, Class<?>> compiledObjects;

    private Map<String, String> tableClasses = new HashMap<String, String>();
    private Map<String, String> securityClasses = new HashMap<String, String>();

    /**
     * generate java classes from dynamic config.
     * @param path to hjson config
     */
    public ElideDynamicEntityCompiler(String path) {
        try {
            elideConfigParser.parseConfigPath(path);

            ElideTableConfig tableConfig = elideConfigParser.getElideTableConfig();
            ElideSecurityConfig securityConfig = elideConfigParser.getElideSecurityConfig();
            HandlebarsHydrator handlebarHydrator = new HandlebarsHydrator();

            tableClasses = handlebarHydrator.hydrateTableTemplate(tableConfig);
            securityClasses = handlebarHydrator.hydrateSecurityTemplate(securityConfig);

            for (Entry<String, String> entry : tableClasses.entrySet()) {
                classNames.add(PACKAGE_NAME + entry.getKey());
            }

            for (Entry<String, String> entry : securityClasses.entrySet()) {
                classNames.add(PACKAGE_NAME + entry.getKey());
            }

            inMemoryJavaCompiler.useParentClassLoader(
                    new ElideDynamicInMemoryClassLoader(ClassLoader.getSystemClassLoader(),
                            Sets.newHashSet(classNames)));

        } catch (Exception e) {
            log.error("Unable to read Dynamic Configuration " + e.getMessage());
        }

    }

    /**
     * compile dynamic classes in-memory.
     */
    public void compile() {

        try {
            for (Map.Entry<String, String> tablePojo : tableClasses.entrySet()) {
                log.info("key: " + PACKAGE_NAME + tablePojo.getKey() + ", value: " + tablePojo.getValue());
                inMemoryJavaCompiler.addSource(PACKAGE_NAME + tablePojo.getKey(), tablePojo.getValue());
            }

            for (Map.Entry<String, String> secPojo : securityClasses.entrySet()) {
                log.info("key: " + PACKAGE_NAME +  secPojo.getKey() + ", value: " + secPojo.getValue());
                inMemoryJavaCompiler.addSource(PACKAGE_NAME + secPojo.getKey(), secPojo.getValue());
            }
            compiledObjects = inMemoryJavaCompiler.compileAll();
        } catch (Exception e) {
            log.error("Unable to compile dynamic classes");
        }
    }

    /**
     * getter for classLoader.
     * @return ClassLoader
     */
    public ClassLoader getClassLoader() {
        return inMemoryJavaCompiler.getClassloader();
    }

    /**
     * getter for classed to be bound.
     * @return set of classes to be bound, Dynamic and Declared
     */
    @SuppressWarnings("rawtypes")
    public Set<Class> getBindClasses() {
        return bindClasses;
    }
}
