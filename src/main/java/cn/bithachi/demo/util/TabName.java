package cn.bithachi.demo.util;


import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface TabName {

    String table();
    String condition() default "NAN";
}
