## DataAggregation

  基于插件化思想实现的数据源插件，实现多源异构的数据采集工具，实现架构借鉴了DataX。

  简化配置，可集成的使用方式，可单独使用data-source-plugins数据源插件做直接通用操作。

## 展望
- [ ] 实现基本框架，core plugins-loader-center插件加载中心，热插拔。
- [ ] 一步一步实现data-source-plugins，使基本的数据源类型（rdbms,mq,file system等）都被抽象成可加载的插件。
- [ ] 围绕着data-source-plugins进一步封装，实现etl的基本架构，reader-transformer-writer。
- [ ] 实现拓展插件类型，transformer等。
- [ ] 在etl的基本架构上，实现多源合并，数据广播，并且实现DAG工作流。
- [ ] 实现分布式任务，负载均衡，故障转移，重试，检查点等。
- [ ] 可植入到springboot，实现starter自动装载。

