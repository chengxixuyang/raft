package cn.seagull.util;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ServiceLoaderUtil;
import cn.seagull.SPI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ServiceLoaderFactory {

    public static <T> List<T> loadService(Class<T> cls) {
        List<T> candidateServices = ServiceLoaderUtil.loadList(cls);

        List<T> services = new ArrayList<>(candidateServices.size());
        if (CollectionUtil.isNotEmpty(candidateServices)) {
            for (T candidateService : candidateServices) {
                SPI spi = candidateService.getClass().getAnnotation(SPI.class);
                if (spi != null) {
                    services.add(candidateService);
                }
            }

            Collections.sort(services, new Comparator<Object>() {
                @Override
                public int compare(Object o1, Object o2) {
                    return o2.getClass().getAnnotation(SPI.class).priority() - o1.getClass().getAnnotation(SPI.class).priority();
                }
            });
        }

        return services;
    }

    public static <T> T loadFirstService(Class<T> cls) {
        List<T> services = loadService(cls);
        if (CollectionUtil.isNotEmpty(services)) {
            return services.getFirst();
        }

        return null;
    }
}
