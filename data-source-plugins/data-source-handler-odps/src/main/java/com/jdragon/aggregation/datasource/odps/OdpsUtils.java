package com.jdragon.aggregation.datasource.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;

public class OdpsUtils {
    public static Odps createOdps(BaseDataSourceDTO dataSource) {
        Account account = new AliyunAccount(dataSource.getUserName(), dataSource.getPassword());
        Odps odps = new Odps(account);
        odps.setEndpoint(dataSource.getHost());
        odps.setDefaultProject(dataSource.getDatabase());
        return odps;
    }
}
