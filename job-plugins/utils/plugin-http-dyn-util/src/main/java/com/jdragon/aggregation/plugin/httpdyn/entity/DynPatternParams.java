package com.jdragon.aggregation.plugin.httpdyn.entity;

/**
 * @author hjs
 * @version 1.0
 * @date 2020/5/8 10:20
 */
public class DynPatternParams {

  private String originalParams;

  private String executeCode;

  private String[] paramStrArr;

  public String getOriginalParams() {
    return originalParams;
  }

  public void setOriginalParams(String originalParams) {
    this.originalParams = originalParams;
  }

  public String getExecuteCode() {
    return executeCode;
  }

  public void setExecuteCode(String executeCode) {
    this.executeCode = executeCode;
  }

  public String[] getParamStrArr() {
    return paramStrArr;
  }

  public void setParamStrArr(String[] paramStrArr) {
    this.paramStrArr = paramStrArr;
  }

  public DynPatternParams() {
  }

  public DynPatternParams(String originalParams, String executeCode, String[] paramStrArr) {
    this.originalParams = originalParams;
    this.executeCode = executeCode;
    this.paramStrArr = paramStrArr;
  }
}
