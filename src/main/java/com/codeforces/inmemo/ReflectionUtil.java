package com.codeforces.inmemo;

import net.sf.cglib.reflect.FastClass;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
final class ReflectionUtil {
    private static final ConcurrentMap<Class<?>, FastClass> fastClasses = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, String> tableClassNamesCache = new ConcurrentHashMap<>();

    private ReflectionUtil() {
        // No operations.
    }

    public static <T> T newInstance(Class<T> clazz) {
        FastClass fastClass = fastClasses.get(clazz);
        if (fastClass == null) {
            fastClasses.put(clazz, FastClass.create(clazz));
            fastClass = fastClasses.get(clazz);
        }

        try {
            //noinspection unchecked
            return (T) fastClass.newInstance();
        } catch (InvocationTargetException e) {
            throw new InmemoException("Can't create new instance [class=" + clazz + "].", e);
        }
    }

    public static String getTableClassSpec(Class<?> clazz) {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);

            StringBuilder result = new StringBuilder();
            for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
                result.append(propertyDescriptor.getName())
                        .append(':')
                        .append(propertyDescriptor.getPropertyType().getName())
                        .append(',');
            }

            return result.toString();
        } catch (IntrospectionException e) {
            throw new InmemoException("Can't get class specification [class=" + clazz + "].", e);
        }
    }

    public static String getTableClassName(Class<?> clazz) {
        String cachedResult = tableClassNamesCache.get(clazz);
        if (cachedResult != null) {
            return cachedResult;
        }

        Class<?> currentClass = clazz;

        while (currentClass != null) {
            String currentClassName = currentClass.getName();
            if (currentClassName.contains("$")) {
                currentClass = currentClass.getSuperclass();
            } else {
                tableClassNamesCache.putIfAbsent(clazz, currentClassName);
                return currentClassName;
            }
        }

        throw new InmemoException("Can't get table class name [class=" + clazz + "].");
    }
}
