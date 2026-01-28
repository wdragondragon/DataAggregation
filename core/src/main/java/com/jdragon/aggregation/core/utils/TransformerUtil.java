package com.jdragon.aggregation.core.utils;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.core.transformer.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TransformerUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TransformerUtil.class);

    public static List<TransformerExecution> buildTransformerInfo(Configuration taskConfig, List<Transformer> extraTransformer) {
        List<Configuration> tfConfigs = taskConfig.getListConfiguration(ParamsKey.JOB_TRANSFORMER);
        if (tfConfigs == null || tfConfigs.isEmpty()) {
            return new ArrayList<>();
        }

        List<TransformerExecution> result = new ArrayList<>();


        List<String> functionNames = new ArrayList<>();


        for (Configuration configuration : tfConfigs) {
            String functionName = configuration.getString("name");
            if (StringUtils.isEmpty(functionName)) {
                throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR, "config=" + configuration.toJSON());
            }

            if (functionName.equals("dx_groovy") && functionNames.contains("dx_groovy")) {
                throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR, "dx_groovy can be invoke once only.");
            }
            functionNames.add(functionName);
        }


        /**
         * 延迟load 第三方插件的function，并按需load
         */
        LOG.info(String.format(" user config tranformers [%s], loading...", functionNames));
        TransformerRegistry.loadTransformerFromLocalStorage(functionNames);


        int i = 0;

        for (Configuration configuration : tfConfigs) {
            String functionName = configuration.getString("name");
            TransformerInfo transformerInfo = TransformerRegistry.getTransformer(functionName);
            if (transformerInfo == null) {
                Optional<Transformer> first = extraTransformer.stream().filter(e -> e.getTransformerName().equals(functionName)).findFirst();
                if (first.isPresent()) {
                    Transformer transformer = first.get();
                    transformerInfo = new TransformerInfo();
                    transformerInfo.setTransformer(transformer);
                    transformerInfo.setClassLoader(Thread.currentThread().getContextClassLoader());
                    transformerInfo.setNative(false);
                } else {
                    throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_NOTFOUND_ERROR, "name=" + functionName);
                }
            }

            /**
             * 具体的UDF对应一个paras
             */
            TransformerExecutionParas transformerExecutionParas = new TransformerExecutionParas();
            /**
             * groovy function仅仅只有code
             */
            if (!functionName.equals("dx_groovy") && !functionName.equals("dx_fackGroovy")) {
                Integer columnIndex = configuration.getInt(ParamsKey.TRANSFORMER_PARAMETER_COLUMNINDEX);

                if (columnIndex == null) {
                    throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "columnIndex must be set by UDF:name=" + functionName);
                }

                transformerExecutionParas.setColumnIndex(columnIndex);
                List<String> paras = configuration.getList(ParamsKey.TRANSFORMER_PARAMETER_PARAS, String.class);
                if (paras != null && !paras.isEmpty()) {
                    transformerExecutionParas.setParas(paras.toArray(new String[0]));
                }
            } else {
                String code = configuration.getString(ParamsKey.TRANSFORMER_PARAMETER_CODE);
                if (StringUtils.isEmpty(code)) {
                    throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "groovy code must be set by UDF:name=" + functionName);
                }
                transformerExecutionParas.setCode(code);

                List<String> extraPackage = configuration.getList(ParamsKey.TRANSFORMER_PARAMETER_EXTRAPACKAGE, String.class);
                if (extraPackage != null && !extraPackage.isEmpty()) {
                    transformerExecutionParas.setExtraPackage(extraPackage);
                }
            }
            transformerExecutionParas.setTContext(configuration.getMap(ParamsKey.TRANSFORMER_PARAMETER_CONTEXT));

            TransformerExecution transformerExecution = new TransformerExecution(transformerInfo, transformerExecutionParas);

            transformerExecution.genFinalParas();
            result.add(transformerExecution);
            i++;
            LOG.info(String.format(" %s of transformer init success. name=%s, isNative=%s parameter = %s"
                    , i, transformerInfo.getTransformer().getTransformerName()
                    , transformerInfo.isNative(), configuration.getConfiguration("parameter")));
        }


        return result;
    }
}
