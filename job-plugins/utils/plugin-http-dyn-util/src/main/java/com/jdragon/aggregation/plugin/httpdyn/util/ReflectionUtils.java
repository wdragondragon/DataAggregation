package com.jdragon.aggregation.plugin.httpdyn.util;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.*;

/**
 * @author hjs
 * @version 1.0
 * @date 2020/5/4 21:57
 */
@Slf4j
public class ReflectionUtils {

    /**
     * 提指定的类载入以系统中
     *
     * @param name 类名称
     * @return 类对象
     * @throws ClassNotFoundException
     */
    public static Class<?> classForName(String name) throws ClassNotFoundException {
        try {
            return Thread.currentThread().getContextClassLoader().loadClass(name);
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        }
        return Class.forName(name);
    }

    /**
     * 根据名称生成指定的对象
     *
     * @param name 类名称
     * @return 具体的对象, 若发生异常，则返回null
     */
    public static Object objectForName(String name) {
        try {
            return Class.forName(name).newInstance();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 直接读取对象的属性值, 忽略 private/protected 修饰符, 也不经过 getter
     */
    public static Object getFieldValue(Object object, String fieldName) throws IllegalAccessException {
        Field field = getDeclaredField(object, fieldName);
        if (field == null) {
            throw new IllegalArgumentException("Could not find field [" + fieldName + "] on target [" + object + "]");
        }
        makeAccessible(field);
        return field.get(object);
    }

    /**
     * 直接设置对象属性值, 忽略 private/protected 修饰符, 也不经过 setter
     */
    public static void setFieldValue(Object object, String fieldName, Object value) throws IllegalAccessException {
        Field field = getDeclaredField(object, fieldName);

        if (field == null) {
            throw new IllegalArgumentException("Could not find field [" + fieldName + "] on target [" + object + "]");
        }
        makeAccessible(field);
        field.set(object, value);
    }


    /**
     * 通过反射, 获得定义 Class 时声明的父类的泛型参数的类型
     * 如: public EmployeeDao extends BaseDao<Employee, String>
     */
    @SuppressWarnings("unchecked")
    public static Class getSuperClassGenricType(Class clazz, int index) {
        Type genType = clazz.getGenericSuperclass();

        if (!(genType instanceof ParameterizedType)) {
            return Object.class;
        }

        Type[] params = ((ParameterizedType) genType).getActualTypeArguments();

        if (index >= params.length || index < 0) {
            return Object.class;
        }

        if (!(params[index] instanceof Class)) {
            return Object.class;
        }

        return (Class) params[index];
    }

    /**
     * 通过反射, 获得 Class 定义中声明的父类的泛型参数类型
     * 如: public EmployeeDao extends BaseDao<Employee, String>
     *
     * @param <T>
     * @param clazz
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> getSuperGenericType(Class clazz) {
        return getSuperClassGenricType(clazz, 0);
    }

    /**
     * 循环向上转型, 获取对象的 DeclaredMethod
     *
     * @param object         对象
     * @param methodName     方法名
     * @param parameterTypes
     * @return
     */
    public static Method getDeclaredMethod(Object object, String methodName, Class<?>[] parameterTypes) {

        for (Class<?> superClass = object.getClass(); superClass != Object.class; superClass = superClass.getSuperclass()) {
            try {
                return superClass.getDeclaredMethod(methodName, parameterTypes);
            } catch (NoSuchMethodException e) {
                //Method 不在当前类定义, 继续向上转型
            }
        }

        return null;
    }

    /**
     * 使 filed 变为可访问
     *
     * @param field 参数
     */
    public static void makeAccessible(Field field) {
        if (!Modifier.isPublic(field.getModifiers())) {
            field.setAccessible(true);
        }
    }

    /**
     * 循环向上转型, 获取对象的 DeclaredField
     *
     * @param object    对象
     * @param filedName 参数名
     * @return 参数
     */
    public static Field getDeclaredField(Object object, String filedName) {

        for (Class<?> superClass = object.getClass(); superClass != Object.class; superClass = superClass.getSuperclass()) {
            try {
                return superClass.getDeclaredField(filedName);
            } catch (NoSuchFieldException e) {
                //Field 不在当前类定义, 继续向上转型
            }
        }
        return null;
    }

    /**
     * 直接调用对象方法, 而忽略修饰符(private, protected)
     *
     * @param object         对象
     * @param methodName     方法名
     * @param parameterTypes 参数类型
     * @param parameters     传参
     * @return Object
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     */
    public static Object invokeMethod(Object object, String methodName, Class<?>[] parameterTypes,
                                      Object[] parameters) throws InvocationTargetException, IllegalAccessException {
        Method method = getDeclaredMethod(object, methodName, parameterTypes);
        if (method == null) {
            throw new IllegalArgumentException("Could not find method [" + methodName + "] on target [" + object + "]");
        }
        method.setAccessible(true);
        return method.invoke(object, parameters);
    }

    /**
     * 根据方法名调用指定对象的方法
     *
     * @param object 要调用方法的对象
     * @param method 要调用的方法名
     * @param args   参数对象数组
     * @return
     */
    public static Object invoke(Object object, String method, Object... args) {
        Object result = null;
        Class<? extends Object> clazz = object.getClass();
        Method queryMethod = getMethod(clazz, method, args);
        if (queryMethod != null) {
            try {
                result = queryMethod.invoke(object, args);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                log.error(e.getMessage(), e);
            }
        } else {
            try {
                throw new NoSuchMethodException(clazz.getName() + " 类中没有找到 " + method + " 方法。");
            } catch (NoSuchMethodException e) {
                log.error(e.getMessage(), e);
            }
        }
        return result;
    }

    /**
     * 根据方法名和参数对象查找方法
     *
     * @param clazz
     * @param name
     * @param args  参数实例数据
     * @return
     */
    public static Method getMethod(Class<? extends Object> clazz, String name, Object[] args) {
        Method queryMethod = null;
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (method.getName().equals(name)) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                // 判断传递的参数个数是否与方法参数的个数相同，可能存在重载的方法
                if (parameterTypes.length == args.length) {
                    boolean isSameMethod = true;
                    // 判断传递的参数类型是否与方法参数类型相同
                    for (int i = 0; i < parameterTypes.length; i++) {
                        Object arg = args[i];
                        if (arg == null) {
                            arg = "";
                        }
                        if (!parameterTypes[i].equals(args[i].getClass())) {
                            isSameMethod = false;
                        }
                    }
                    if (isSameMethod) {
                        queryMethod = method;
                        break;
                    }
                }
            }
        }
        return queryMethod;
    }

    public static Method getDynMethod(Class<? extends Object> clazz, String name) {
        Method method = null;
        try {
            method = clazz.getMethod(name, Object[].class);
        } catch (NoSuchMethodException e) {
            log.error(e.getMessage(), e);
        }
        return method;
    }

    /**
     * @param className 类名
     * @param method    方法名
     * @param args      参数
     * @return 返回
     */
    public static Object invokeByClassName(String className, String method, Object... args) {
        Object object = objectForName(className);
        Object result = null;
        Class<? extends Object> clazz = object.getClass();
        Method queryMethod = getDynMethod(clazz, method);
        if (queryMethod != null) {
            try {
                result = queryMethod.invoke(object, args);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                log.error(e.getMessage(), e);
            }
        } else {
            try {
                throw new NoSuchMethodException(clazz.getName() + " 类中没有找到 " + method + " 方法。");
            } catch (NoSuchMethodException e) {
                log.error(e.getMessage(), e);
            }
        }
        return result;
    }
}
