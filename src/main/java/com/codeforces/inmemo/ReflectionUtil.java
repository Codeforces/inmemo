package com.codeforces.inmemo;

import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;

import javax.annotation.Nonnull;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author MikeMirzayanov (mirzayanovmr@gmail.com)
 */
final class ReflectionUtil {
    private static final ConcurrentMap<Class<?>, FastClass> fastClassCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, Map<String, FastMethod>> gettersCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, Map<String, FastMethod>> settersCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, String> tableClassNamesCache = new ConcurrentHashMap<>();

    private ReflectionUtil() {
        // No operations.
    }

    public static <T> T newInstance(Class<T> clazz) {
        FastClass fastClass = getFastClass(clazz);

        try {
            //noinspection unchecked
            return (T) fastClass.newInstance();
        } catch (InvocationTargetException e) {
            throw new InmemoException("Can't create new instance [class=" + clazz + "].", e);
        }
    }

    @Nonnull
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

    private static FastClass getFastClass(Class<?> clazz) {
        FastClass result = fastClassCache.get(clazz);

        while (result == null) {
            FastClass fastClass = FastClass.create(clazz);
            fastClassCache.putIfAbsent(clazz, fastClass);
            result = fastClassCache.get(clazz);
        }

        return result;
    }

    private static Map<String, FastMethod> getGettersMap(Class<?> clazz) {
        Map<String, FastMethod> result = gettersCache.get(clazz);

        while (result == null) {
            result = buildGettersMap(clazz);
            gettersCache.putIfAbsent(clazz, result);
            result = gettersCache.get(clazz);
        }

        return result;
    }

    private static Map<String, FastMethod> buildGettersMap(Class<?> clazz) {
        Map<String, FastMethod> result = new HashMap<>();

        FastClass fastClass = getFastClass(clazz);
        Method[] methods = clazz.getMethods();

        for (Method method : methods) {
            if (!Modifier.isStatic(method.getModifiers())) {
                String property = getGetterProperty(method);
                if (property != null) {
                    result.put(property, fastClass.getMethod(method));
                }
            }
        }

        return result;
    }

    private static Map<String, FastMethod> getSettersMap(Class<?> clazz) {
        Map<String, FastMethod> result = settersCache.get(clazz);

        while (result == null) {
            result = buildSettersMap(clazz);
            settersCache.putIfAbsent(clazz, result);
            result = settersCache.get(clazz);
        }

        return result;
    }

    private static Map<String, FastMethod> buildSettersMap(Class<?> clazz) {
        Map<String, FastMethod> result = new HashMap<>();

        FastClass fastClass = getFastClass(clazz);
        Method[] methods = clazz.getMethods();

        for (Method method : methods) {
            if (!Modifier.isStatic(method.getModifiers())) {
                String property = getSetterProperty(method);
                if (property != null) {
                    result.put(property, fastClass.getMethod(method));
                }
            }
        }

        return result;
    }

    private static String getGetterProperty(Method method) {
        if (method.getParameterTypes().length > 0 || method.getDeclaringClass() == Object.class) {
            return null;
        }

        String name = method.getName();
        if (name.length() > 3 && name.startsWith("get") && Character.isUpperCase(name.charAt(3))) {
            return Character.toLowerCase(name.charAt(3)) + name.substring(4);
        }
        if (name.length() > 2 && name.startsWith("is") && Character.isUpperCase(name.charAt(2))) {
            return Character.toLowerCase(name.charAt(2)) + name.substring(3);
        }

        return null;
    }

    private static String getSetterProperty(Method method) {
        if (method.getParameterTypes().length != 1 || method.getDeclaringClass() == Object.class) {
            return null;
        }

        String name = method.getName();
        if (name.length() > 3 && name.startsWith("set") && Character.isUpperCase(name.charAt(3))) {
            return Character.toLowerCase(name.charAt(3)) + name.substring(4);
        }

        return null;
    }

    public static void copyProperties(Object source, Object target) {
        if (source == target) {
            return;
        }

        if (source == null) {
            throw new NullPointerException("Argument source can't be null (if target is not null).");
        }

        if (target == null) {
            throw new NullPointerException("Argument target can't be null (if source is not null).");
        }

        Map<String, FastMethod> sourceGetters = getGettersMap(source.getClass());
        Map<String, FastMethod> targetSetters = getSettersMap(target.getClass());

        for (Map.Entry<String, FastMethod> getterEntry : sourceGetters.entrySet()) {
            FastMethod getter = getterEntry.getValue();
            FastMethod setter = targetSetters.get(getterEntry.getKey());

            if (setter != null) {
                Class<?> getterReturnsClass = getter.getReturnType();
                Class<?> setterExpectsClass = setter.getParameterTypes()[0];

                Object value;
                try {
                    value = getter.invoke(source, new Object[]{});
                } catch (InvocationTargetException e) {
                    throw new RuntimeException("Can't get property '" + getterEntry.getKey()
                            + "' from " + source.getClass() + ".", e);
                }

                if (setterExpectsClass.isAssignableFrom(getterReturnsClass)) {
                    try {
                        setter.invoke(target, new Object[]{value});
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException("Can't copy assignable property '" + getterEntry.getKey()
                                + "' from " + source.getClass() + " to " + target.getClass() + ".", e);
                    }
                    continue;
                }

                if (setterExpectsClass.isAssignableFrom(String.class)) {
                    try {
                        if (value == null) {
                            setter.invoke(target, new Object[]{null});
                        } else {
                            setter.invoke(target, new Object[]{value.toString()});
                        }
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException("Can't copy assignable property '" + getterEntry.getKey()
                                + "' from " + source.getClass()
                                + " to string assignable property of " + target.getClass() + ".", e);
                    }
                    continue;
                }

                if (setterExpectsClass.isEnum()) {
                    try {
                        if (value == null) {
                            setter.invoke(target, new Object[]{null});
                        } else {
                            String valueString = value.toString();
                            Object[] constants = setterExpectsClass.getEnumConstants();
                            for (Object constant : constants) {
                                if (constant.toString().equals(valueString)) {
                                    setter.invoke(target, new Object[]{constant});
                                    break;
                                }
                            }
                        }
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException("Can't copy enum property '" + getterEntry.getKey()
                                + "' from " + source.getClass()
                                + " to " + target.getClass() + ".", e);
                    }
                    continue;
                }

                if (value != null) {
                    try {
                        Object valueCopy = setterExpectsClass.newInstance();
                        copyProperties(value, valueCopy);
                        setter.invoke(target, new Object[]{valueCopy});
                    } catch (Exception e) {
                        throw new RuntimeException("Can't copy object property '" + getterEntry.getKey()
                                + "' from " + source.getClass()
                                + " to " + target.getClass() + ".", e);
                    }
                }
            }
        }
    }
}
