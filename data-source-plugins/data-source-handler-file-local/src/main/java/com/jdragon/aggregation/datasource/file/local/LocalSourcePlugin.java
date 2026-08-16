package com.jdragon.aggregation.datasource.file.local;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.LocalFileHelper;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class LocalSourcePlugin extends LocalFileHelper {

    @Override
    public boolean connect(Configuration configuration) {
        String rootPath = configuration == null ? null : configuration.getString("rootPath");
        List<String> allowedRoots = configuration == null
                ? List.of()
                : configuration.getList("allowedRoots", String.class);
        if (StringUtils.isBlank(rootPath) && (allowedRoots == null || allowedRoots.isEmpty())) {
            throw new IllegalArgumentException("local source requires rootPath or allowedRoots");
        }
        return super.connect(configuration);
    }
}
