#### 描述: 本地数据缓冲合并处理组件。

#### Get Started

```pom
<dependency>
    <groupId>com.github.andy</groupId>
    <artifactId>buffer-processor</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

#### Usage

```Java
    
// 创建Shard缓冲处理器
ShardBufferProcessor<TestElement, Long, String> shardBufferProcessor = ShardBufferProcessor.<TestElement, Long, String>newBuilder()
                                                                                       .bufferQueueSize(3000) //
                                                                                       .consumeBatchSize(20) //
                                                                                       .consumeWaitTimeoutMs(10) //
                                                                                       .bufferGroupStrategy(new TestBufferGroupStrategy()) //
                                                                                       .bufferGroupHandler(new TestBufferGroupHandler()) //
                                                                                       //.bufferProcessExecutorFactory()
                                                                                       .shardBufferProcessorSize(2) //
                                                                                       .shardBufferProcessorStrategy(new TestShardBufferProcessorStrategy()) //
                                                                                       .build();
       
// 准备处理的对象
TestElement element = new TestElement();
element.setId(1);
element.setName("张三");
       
// 提交处理对象到缓冲处理器处理并获取Future
BufferFuture<String> future = shardBufferProcessor.submit(element);
       
// 通过future获取结果
String result = future.get();
```

```Java
    
// 处理对象
public class TestElement {
    
    private int id;
    
    private String name;
    
    public int getId() {
        return id;
    }
    
    public void setId(int id) {
        this.id = id;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
}
       
// 缓冲分组策略类
public class TestBufferGroupStrategy implements BufferGroupStrategy<TestElement, Long> {
    
    @Override
    public Long doGroup(final TestElement element) throws Exception{
        return (long) (element.getId() % 10);
    }
}
       
// 缓冲分组后处理类
public class TestBufferGroupHandler implements BufferGroupHandler<TestElement, Long, String> {
    
    @Override
    public Map<TestElement, String> handle(final Long group, final List<TestElement> elements) throws Exception {
        Map<TestElement, String> resultMap = Maps.newHashMap();
        for (TestElement testElement : elements) {
            resultMap.put(testElement, testElement.getName());
        }
        return resultMap;
    }
}
       
// 分片缓冲路由策略
public class TestShardBufferProcessorStrategy implements ShardBufferProcessorStrategy<TestElement> {
   
    @Override
    public int routeIDX(int processorsCount, TestElement element) {
        return (int)element.getId() % processorsCount;
    }
}
```
