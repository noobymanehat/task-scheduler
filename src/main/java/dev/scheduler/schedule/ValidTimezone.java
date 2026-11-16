package dev.scheduler.schedule;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** A zone id the JVM knows, such as {@code UTC} or {@code Europe/Berlin}. Null is allowed. */
@Target({ElementType.FIELD, ElementType.RECORD_COMPONENT, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = ValidTimezone.Validator.class)
public @interface ValidTimezone {

    String message() default "must be a valid timezone id such as 'UTC' or 'Europe/Berlin'";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    class Validator implements ConstraintValidator<ValidTimezone, String> {
        @Override
        public boolean isValid(String value, ConstraintValidatorContext context) {
            return value == null || CronCalculator.isValidTimezone(value);
        }
    }
}
