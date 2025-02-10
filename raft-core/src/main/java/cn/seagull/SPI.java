package cn.seagull;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
@Documented
public @interface SPI {

    String name() default "";

    int priority() default 0;
}
