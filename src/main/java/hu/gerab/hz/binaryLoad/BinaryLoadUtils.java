package hu.gerab.hz.binaryLoad;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Objects;

class BinaryLoadUtils {

    private BinaryLoadUtils() {
    }

    public static boolean isAnnotationPresent(Class<? extends Annotation> annotationClass, Object... instances) {
        return Arrays.stream(instances)
                .filter(Objects::nonNull)
                .map(Object::getClass)
                .anyMatch(clazz -> isAnnotationPresent(clazz, annotationClass));
    }

    /**
     * This is a based on the SpringFramework AnnotationUtils::findAnnotation method, to deal with proxies by frameworks
     *
     * @param clazz           the class on which to search the annotation ( and all superclasses )
     * @param annotationClass the annotation to search for
     * @return whether the specified annotation is present on the supplied class or any superclasses
     */
    private static boolean isAnnotationPresent(Class<?> clazz, Class<? extends Annotation> annotationClass) {
        for (Annotation annotation : clazz.getDeclaredAnnotations()) {
            if (annotation.annotationType() == annotationClass) {
                return true;
            }
        }

        for (Class<?> iface : clazz.getInterfaces()) {
            boolean annotationPresent = isAnnotationPresent(iface, annotationClass);
            if (annotationPresent) {
                return true;
            }
        }

        Class<?> superclass = clazz.getSuperclass();
        if (superclass == null || Object.class == superclass) {
            return false;
        }
        return isAnnotationPresent(superclass, annotationClass);
    }
}
