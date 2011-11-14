package com.geishatokyo.helenos.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: takeshita
 * Create: 11/11/14 16:52
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ASuperColumn {
    String keyspace();
    String columnFamily();

}
