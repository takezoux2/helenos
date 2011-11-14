package com.geishatokyo.helenos.annotation;

/**
 * User: takeshita
 * Create: 11/11/14 17:09
 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD,ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AColumn {

    String columnName() default "";


}
