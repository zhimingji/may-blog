Flink Async I/O
一句话：让一个算子实例同时处理一批记录的外部请求，而不是一条一条排队等。同步写法在等网络时 CPU 干瞪眼；异步写法让 CPU 在等待期间继续处理下一条。

一、什么场景需要它
只要满足「每条记录都要发一次外部请求，且这个请求耗时远大于本地计算」，就适合用 Async I/O：

典型场景	外部请求
维表关联（订单流 join 用户表）	查 Redis / MySQL
调用第三方 API	风控校验、地址解析、物流查询、IP 归属地
微服务间调用	RPC / HTTP 查其他服务
多数据源富化	一条主数据拼多个外部来源
缓存查询	大 key 点查、配置中心拉取
同步写法为什么不行：一个算子实例在某一刻只能处理一条记录，调用外部接口时整个线程卡在等结果上，CPU 空转。假设并行度 8、一次调用往返耗时 50ms，吞吐上限 ≈ 8 ÷ 0.05 = 160 条/秒。异步写法同时挂 100 个未返回的请求，吞吐可到 16000 条/秒，差两个数量级。

二、记住四步：建 → 发 → 回 → 接
这四步就是一次异步请求从准备到落地的完整过程，也是你写代码从上到下的顺序。

步骤	对应 API	做什么	记忆要点
建	open() / close()	建客户端、建线程池	每个 subtask 一份，必须有界（固定大小，不能无限扩）
发	asyncInvoke()	发起请求，立即返回	方法里绝不阻塞，不能等结果
回	complete() / timeout()	结果回调、超时兜底	成功失败都必须回调，否则那条记录永远不出
接	AsyncDataStream.unorderedWait()	接入数据流，配两个参数	timeout（超时）+ capacity（最多同时挂几个请求）
三、完整 Demo（单文件，无外部依赖）
java
import org.apache.flink.api.common.functions.RichAsyncFunction;
import org.apache.flink.api.common.functions.ResultFuture;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.concurrent.*;

public class AsyncIoDemo {

    public static class AsyncEnrich extends RichAsyncFunction<String, String> {
        private transient ExecutorService pool;

        // ===== 建：资源在这里建，每个 subtask 一份 =====
        @Override
        public void open(Configuration cfg) {
            pool = Executors.newFixedThreadPool(16);   // 有界线程池，防线程爆炸
        }

        // ===== 发：只发请求，立即返回 =====
        @Override
        public void asyncInvoke(String input, ResultFuture<String> future) {
            CompletableFuture.supplyAsync(() -> {
                sleep(50);                       // 模拟一次外部请求的往返耗时
                return input + " -> VIP";        // 模拟查询结果
            }, pool).whenComplete((result, ex) -> {
                // ===== 回：成功、失败都必须回调 =====
                if (ex != null) future.completeExceptionally(ex);
                else            future.complete(Collections.singletonList(result));
            });
        }

        // ===== 回：超时兜底，默认超时会挂掉作业，重写后"降级不挂" =====
        @Override
        public void timeout(String input, ResultFuture<String> future) {
            future.complete(Collections.singletonList(input + " -> TIMEOUT"));
        }

        @Override
        public void close() {
            if (pool != null) pool.shutdown();
        }

        private static void sleep(long ms) {
            try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> orders = env.fromElements("o1", "o2", "o3");

        // ===== 接：接入流，给两个关键参数 =====
        DataStream<String> enriched = AsyncDataStream.unorderedWait(
                orders,
                new AsyncEnrich(),
                2, TimeUnit.SECONDS,   // timeout：单条请求等多久算超时
                100                    // capacity：这个 subtask 最多同时挂 100 个未返回的请求
        );

        enriched.print();
        env.execute("Async I/O Demo");
    }
}
只需 Flink 两个依赖即可跑通：flink-streaming-java + flink-clients。

四、orderedWait 还是 unorderedWait？
unorderedWait（推荐）	orderedWait
输出顺序	谁先回来谁先输出	与输入顺序严格一致
内存占用	小	大（要缓存乱序结果等前面的）
吞吐	高	低
什么时候用	绝大多数场景	必须同 key 严格保序（如账户流水）
默认选 unorderedWait，除非业务明确要求顺序。

五、五个必踩的坑
asyncInvoke 里写了阻塞（future.get() / Thread.sleep()）→ 直接退回同步，白折腾。

在 asyncInvoke 里 new 客户端 / 线程池 → 每条记录建一次，连接池爆炸。资源只能在 open() 建、close() 关。

忘记调用 complete() → 这条记录永远不出，作业卡在存档（checkpoint）阶段不动。

capacity / timeout 拍脑袋定 →

capacity 决定"同时挂几个请求"，满了上游会被卡住（叫"反压"）；

timeout 必须 远小于 checkpoint 间隔，因为 checkpoint 要等所有未返回的请求落定，timeout 太长会把存档拖超时。

忘了做幂等 → 作业从存档恢复时，那些"已发出但未返回"的请求会被重放，外部调用可能收到重复。查询天然幂等，但 Async I/O 里如果带写操作，必须自己设计去重键。

六、真实客户端怎么选
上面的 Demo 用「线程池 + CompletableFuture」模拟，是降级方案。生产优先选原生支持异步回调的客户端：

外部系统	推荐	不要用
Redis	Lettuce（Netty 驱动）	Jedis 同步 API
HTTP	OkHttp / Vert.x WebClient	HttpURLConnection
MySQL	Vert.x MySQL Client / R2DBC	JDBC（同步）
MongoDB	官方 Reactive Driver	同步 Driver
没有原生异步客户端时，才用「有界线程池 + CompletableFuture」包装，但绝不能用 Executors.newCachedThreadPool()。

七、Flink SQL 里的 Async I/O
维表 Lookup Join 也能开异步，三条配置搞定：

sql
SET table.exec.async-lookup.output-mode = ALLOW_UNORDERED;
SET table.exec.async-lookup.buffer-capacity = 100;   -- 对应 capacity
SET table.exec.async-lookup.timeout = 30s;           -- 对应 timeout
配合实现异步版 LookupFunction（重写 asyncLookup），逻辑与 Java 版完全对应。

八、总结
Async I/O 的本质是用并发换吞吐：把"一条记录卡在等外部结果"变成"一批记录同时在路上飞"。记住 建 → 发 → 回 → 接 四步，对照下面清单逐项确认，就是能上生产的代码：

□ 客户端 / 线程池在 open() 里建，close() 里关，每个 subtask 一份，大小有界
□ asyncInvoke() 里只发请求，任何阻塞代码都不许出现
□ 成功、失败都必须调 complete() / completeExceptionally()
□ 重写 timeout() 做降级，不让单条超时拖垮整个作业
□ 用 unorderedWait（除非业务必须保序）
□ timeout 远小于 checkpoint 间隔
□ capacity 按外部系统能扛的并发量设
□ 外部调用保证幂等（存档恢复时可能被重放）
一句话：Async I/O 不是"异步 API"，而是"有界并发"——用一个有限的并发窗口，把 IO 等待从吞吐瓶颈里拿掉。建、发、回、接四步做对，坑避开，量级提升便是水到渠成。
