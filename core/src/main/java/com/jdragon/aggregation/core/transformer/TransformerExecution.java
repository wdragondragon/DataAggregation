package com.jdragon.aggregation.core.transformer;

import com.jdragon.aggregation.core.plugin.Transformer;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
public class TransformerExecution {


    private Object[] finalParas;

    private final TransformerExecutionParas transformerExecutionParas;

    private final TransformerInfo transformerInfo;

    /**
     * 参数采取延迟检查
     */

    @Setter
    private boolean isChecked = false;

    public TransformerExecution(TransformerInfo transformerInfo, TransformerExecutionParas transformerExecutionParas) {
        this.transformerExecutionParas = transformerExecutionParas;
        this.transformerInfo = transformerInfo;
    }


    public void genFinalParas() {
        if (transformerInfo.getTransformer().getTransformerName().equals("dx_groovy")) {
            finalParas = new Object[2];
            finalParas[0] = transformerExecutionParas.getCode();
            finalParas[1] = transformerExecutionParas.getExtraPackage();
            return;
        }

        if (transformerExecutionParas.getColumnIndex() != null) {
            if (transformerExecutionParas.getParas() != null) {
                finalParas = new Object[transformerExecutionParas.getParas().length + 1];
                System.arraycopy(transformerExecutionParas.getParas(), 0, finalParas, 1, transformerExecutionParas.getParas().length);
            } else {
                finalParas = new Object[1];
            }
            finalParas[0] = transformerExecutionParas.getColumnIndex();
        } else {
            if (transformerExecutionParas.getParas() != null) {
                finalParas = transformerExecutionParas.getParas();
            } else {
                finalParas = null;
            }
        }
    }
    /**
     * 一些代理方法
     */
    public ClassLoader getClassLoader() {
        return transformerInfo.getClassLoader();
    }

    public Integer getColumnIndex() {
        return transformerExecutionParas.getColumnIndex();
    }

    public String getTransformerName() {
        return transformerInfo.getTransformer().getTransformerName();
    }

    public Transformer getTransformer() {
        return transformerInfo.getTransformer();
    }

    public Map<String, Object> getTContext() {
        return transformerExecutionParas.getTContext();
    }
}
