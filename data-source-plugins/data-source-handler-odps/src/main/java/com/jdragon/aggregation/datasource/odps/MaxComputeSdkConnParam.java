package com.jdragon.aggregation.datasource.odps;

import lombok.Data;

/**
 * @author itdl
 * @description maxCompute使用SDK的连接参数
 * @date 2022/08/08 10:07
 */
@Data
public class MaxComputeSdkConnParam {
    /**
     * TunnelEndpoint
     */
    private String TunnelEndpoint;

    private String aliyunAccessId;

    private String aliyunAccessKey;

    private String maxComputeEndpoint;

    private String projectName;


}