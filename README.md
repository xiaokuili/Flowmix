 Flowmix - 1天MVP                                                              
                                                                                
  🎯 目标                                                                       
                                                                                
  能跑通一个完整例子：HTTP采集 → LLM处理 → 存储数据库                           
                                                                                
  ---                                                                           
  MVP 功能列表（只做核心）                                                      
                                                                                
  ✅ 必须做（核心流程）                                                         
                                                                                
  1. 任务定义 (2小时)                                                           

  ```                                                                              
  from flowmix import Task                                                      
                                                                                
  task = Task(name="demo")                                                      
                                                                                
  @task.source()                                                                
  async def fetch():                                                            
      yield {"text": "iPhone 16很棒"}                                           
                                                                                
  @task.processor()                                                             
  async def process(item):                                                      
      return {"text": item["text"], "processed": True}                          
                                                                                
  @task.sink()                                                                  
  async def save(item):                                                         
      print(item)                                                               
                                                                                
  task.run()  # 执行                                                            
```                                                                                
  范围：                                                                        
  - ✅ 装饰器：@task.source / @task.processor / @task.sink                      
  - ✅ 自动串联流水线                                                           
  - ✅ 同步执行（先不做异步）                                                   
  - ❌ 调度器（手动运行就行）                                                   
                                                                                
  ---                                                                           
  2. 并发控制（2小时）                                                          
 ```                                                                               
  from flowmix import ConcurrencyLimiter                                        
                                                                                
  limiter = ConcurrencyLimiter(max_workers=5)                                   
                                                                                
  @task.processor(limiter=limiter)                                              
  def process(item):                                                            
      # 自动限制并发                                                            
      return llm_call(item)                                                     
```                                                                                
  范围：                                                                        
  - ✅ 线程池限制并发数                                                         
  - ✅ 简单计数器（不做复杂的rate limiting）                                    
  - ❌ QPM/QPS控制（v2）                                                        
  - ❌ 多任务共享资源池（v2）                                                   
                                                                                
  ---                                                                           
  3. 日志系统（1小时）                                                          
                                                                                
  # 自动记录                                                                    
  [2024-01-20 14:00:00] [INFO] Task started: demo                               
  [2024-01-20 14:00:01] [INFO] Source: fetched 10 items                         
  [2024-01-20 14:00:05] [INFO] Processor: processed 10/10                       
  [2024-01-20 14:00:06] [INFO] Sink: saved 10 items                             
  [2024-01-20 14:00:06] [INFO] Task completed: 6s                               
                                                                                
  范围：                                                                        
  - ✅ 自动打印执行进度                                                         
  - ✅ 基础格式化（时间+级别+消息）                                             
  - ❌ 写文件（先只打印到终端）                                                 
  - ❌ 结构化日志（v2）                                                         
                                                                                
  ---                                                                           
  4. HTTP请求（1.5小时）                                                        
  ```                                                                              
  from flowmix.sources import http_source                                       
                                                                                
  @task.source()                                                                
  def fetch():                                                                  
      return http_source(                                                       
          url="https://api.example.com/data",                                   
          headers={"Authorization": "Bearer xxx"}                               
      )                                                                         
 ```                                                                               
  范围：                                                                        
  - ✅ GET请求                                                                  
  - ✅ 基础认证（headers）                                                      
  - ✅ 返回JSON解析                                                             
  - ❌ POST、分页、重试（v2）                                                   
                                                                                
  ---                                                                           
  5. LLM调用（1.5小时）                                                         
 ```                                                                               
  from flowmix.processors import llm                                            
                                                                                
  @task.processor()                                                             
  def analyze(item):                                                            
      return llm(                                                               
          prompt=f"分析：{item['text']}",                                       
          provider="openai",                                                    
          model="gpt-4"                                                         
      )                                                                         
 ```                                                                               
  范围：                                                                        
  - ✅ OpenAI API调用                                                           
  - ✅ 简单prompt（字符串拼接）                                                 
  - ✅ 返回文本                                                                 
  - ❌ 模板、JSON输出、重试（v2）                                               
                                                                                
  ---                                                                           
  6. 数据库存储（1.5小时）                                                      
 ```                                                                               
  from flowmix.storage import postgres                                          
                                                                                
  @task.sink()                                                                  
  def save(item):                                                               
      postgres.insert(                                                          
          table="results",                                                      
          data=item,                                                            
          connection="postgresql://..."                                         
      )                                                                         
```                                                                                
  范围：                                                                        
  - ✅ PostgreSQL插入                                                           
  - ✅ 自动建表（简单推断类型）                                                 
  - ❌ 批量写入、事务、冲突处理（v2）                                           
                                                                                
  ---                                                                           
  7. 成本统计（1小时）                                                          
                                                                                
  # 执行完自动打印                                                              
  Task completed!                                                               
  Duration: 6s                                                                  
  Cost: $1.25                                                                   
    - OpenAI API: $1.20 (150 calls)                                             
    - Other: $0.05                                                              
                                                                                
  范围：                                                                        
  - ✅ LLM自动统计token成本                                                     
  - ✅ 执行后打印总成本                                                         
  - ❌ 实时统计、导出（v2）                                                     
                                                                                
  ---                                                                           
  8. CLI（0.5小时）                                                             
                                                                                
  flowmix run task.py                                                           
                                                                                
  范围：                                                                        
  - ✅ run 命令执行任务                                                         
  - ❌ 其他命令（v2）                                                           
                                                                                
  ---                                                                           
  ❌ 1天不做                                                                    
                                                                                
  - ❌ 事务管理                                                                 
  - ❌ 数据回滚                                                                 
  - ❌ 调度器（Cron）                                                           
  - ❌ 错误重试                                                                 
  - ❌ 速率限制（QPM）                                                          
  - ❌ 批量处理                                                                 
  - ❌ 配置文件                                                                 
  - ❌ 所有高级功能                                                             
                                                                                
  ---                                                                           
                                                                        
```                                                                       
  完整Demo示例                                                                  
                                                                                
  # demo.py                                                                     
  from flowmix import Task                                                      
  from flowmix.sources import http_source                                       
  from flowmix.processors import llm                                            
  from flowmix.storage import postgres                                          
  from flowmix import ConcurrencyLimiter                                        
  import os                                                                     
                                                                                
  task = Task(name="twitter_sentiment")                                         
                                                                                
  @task.source()                                                                
  def fetch_tweets():                                                           
      """采集推文"""                                                            
      return http_source(                                                       
          url="https://api.twitter.com/2/tweets/search/recent",                 
          headers={"Authorization": f"Bearer {os.getenv('TWITTER_TOKEN')}"},    
          params={"query": "iPhone 16", "max_results": 10}                      
      )                                                                         
                                                                                
  @task.processor(limiter=ConcurrencyLimiter(max_workers=3))                    
  def analyze_sentiment(tweet):                                                 
      """分析情感"""                                                            
      result = llm(                                                             
          prompt=f"分析这条推文的情感（正面/负面/中性）：{tweet['text']}",      
          provider="openai",                                                    
          model="gpt-4"                                                         
      )                                                                         
      return {                                                                  
          "tweet_id": tweet["id"],                                              
          "text": tweet["text"],                                                
          "sentiment": result                                                   
      }                                                                         
                                                                                
  @task.sink()                                                                  
  def save_to_db(data):                                                         
      """存储"""                                                                
      postgres.insert(                                                          
          table="twitter_sentiment",                                            
          data=data,                                                            
          connection=os.getenv('DATABASE_URL')                                  
      )                                                                         
                                                                                
  if __name__ == "__main__":                                                    
      task.run()                                                                
                                                                                
  运行：                                                                        
  export TWITTER_TOKEN=xxx                                                      
  export DATABASE_URL=postgresql://...                                          
  export OPENAI_API_KEY=xxx                                                     
                                                                                
  flowmix run demo.py                                                           
                                                                                
  输出：                                                                        
  [14:00:00] [INFO] Task started: twitter_sentiment                             
  [14:00:01] [INFO] Source: fetched 10 items                                    
  [14:00:15] [INFO] Processor: processed 10/10 (3 concurrent)                   
  [14:00:16] [INFO] Sink: saved 10 items                                        
  [14:00:16] [INFO] Task completed in 16s                                       
  [14:00:16] [INFO] Cost: $0.15 (OpenAI: 1500 tokens)                           
                                                                                
```                                                                          
  项目结构                                                                      
                                                                                
  flowmix/                                                                      
  ├── flowmix/                                                                  
  │   ├── __init__.py                                                           
  │   ├── task.py           # Task类 + 装饰器                                   
  │   ├── concurrency.py    # 并发控制                                          
  │   ├── logger.py         # 日志                                              
  │   ├── sources/                                                              
  │   │   └── http.py       # HTTP请求                                          
  │   ├── processors/                                                           
  │   │   └── llm.py        # LLM调用                                           
  │   └── storage/                                                              
  │       └── postgres.py   # PostgreSQL                                        
  ├── cli.py                # CLI入口                                           
  ├── setup.py                                                                  
  └── examples/                                                                 
      └── demo.py                                                               
                                                                                
  ---                                                                           
  这个1天MVP可行吗？还要调整什么？
