package com.jdragon.aggregation.core.transport.exchanger;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.transformer.TransformerErrorCode;
import com.jdragon.aggregation.core.transformer.TransformerExecution;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.pluginloader.ClassLoaderSwapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * no comments.
 * Created by liqiang on 16/3/9.
 */
public abstract class TransformerExchanger {

    private static final Logger LOG = LoggerFactory.getLogger(TransformerExchanger.class);
    private final List<TransformerExecution> transformerExecs;

    private final ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper
            .newCurrentThreadClassLoaderSwapper();


    public TransformerExchanger(List<TransformerExecution> transformerExecs) {
        this.transformerExecs = transformerExecs;
    }


    public Record doTransformer(Record record) {
        if (transformerExecs == null || transformerExecs.isEmpty()) {
            return record;
        }

        DefaultRecord orignal = (DefaultRecord) record;
        DefaultRecord result = null;
        String errorMsg;
        boolean failed = false;
        for (TransformerExecution transformerInfoExec : transformerExecs) {
            long startTs = System.nanoTime();

            if (transformerInfoExec.getClassLoader() != null) {
                classLoaderSwapper.setCurrentThreadClassLoader(transformerInfoExec.getClassLoader());
            }

            /**
             * 延迟检查transformer参数的有效性，直接抛出异常，不作为脏数据
             * 不需要在插件中检查参数的有效性。但参数的个数等和插件相关的参数，在插件内部检查
             */
            if (!transformerInfoExec.isChecked()) {
                if (transformerInfoExec.getColumnIndex() != null && transformerInfoExec.getColumnIndex() >= record.getColumnNumber()) {
                    throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                            String.format("columnIndex[%s] out of bound[%s]. name=%s",
                                    transformerInfoExec.getColumnIndex(), record.getColumnNumber(),
                                    transformerInfoExec.getTransformerName()));
                }
                transformerInfoExec.setChecked(true);
            }

            try {
                result = (DefaultRecord) transformerInfoExec.getTransformer().evaluate(orignal.clone(), transformerInfoExec.getTContext(), transformerInfoExec.getFinalParas());
            } catch (Exception e) {
                errorMsg = String.format("transformer(%s) has Exception(%s)", transformerInfoExec.getTransformerName(),
                        e.getMessage());
                failed = true;
                LOG.error(errorMsg, e);
                // transformerInfoExec.addFailedRecords(1);
                //脏数据不再进行后续transformer处理，按脏数据处理，并过滤该record。
                break;

            } finally {
                if (transformerInfoExec.getClassLoader() != null) {
                    classLoaderSwapper.restoreCurrentThreadClassLoader();
                }
            }

            if (result == null) {
                /**
                 * 这个null不能传到writer，必须消化掉
                 */
                break;
            }

            long diff = System.nanoTime() - startTs;
            //transformerInfoExec.addExaustedTime(diff);
            //transformerInfoExec.addSuccessRecords(1);
        }

        if (failed) {
            return null;
        } else {
            return result;
        }
    }

    public void doStat() {
        /**
         * todo 对于多个transformer时，各个transformer的单独统计进行显示。最后再汇总整个transformer的时间消耗.
         * 暂时不统计。
         */
//        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS, totalSuccessRecords);
//        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS, totalFailedRecords);
//        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS, totalFilterRecords);
//        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_USED_TIME, totalExaustedTime);
    }


}
