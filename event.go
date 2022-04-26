package event_redis

import (
	"errors"
	"sync"
	"time"

	"github.com/chefsgo/chef"
	"github.com/chefsgo/event"
	"github.com/chefsgo/log"
	"github.com/chefsgo/util"
	"github.com/gomodule/redigo/redis"
)

var (
	errInvalidConnection = errors.New("Invalid event connection.")
	errAlreadyRunning    = errors.New("Redis event is already running.")
)

type (
	redisDriver  struct{}
	redisConnect struct {
		mutex  sync.RWMutex
		client *redis.Pool

		running bool
		actives int64

		name    string
		config  event.Config
		setting redisSetting

		delegate event.Delegate

		events map[string]string
	}
	//配置文件
	redisSetting struct {
		Server   string //服务器地址，ip:端口
		Password string //服务器auth密码
		Database string //数据库

		Idle    int //最大空闲连接
		Active  int //最大激活连接，同时最大并发
		Timeout time.Duration
	}

	defaultMsg struct {
		name string
		data []byte
	}
)

//连接
func (driver *redisDriver) Connect(name string, config event.Config) (event.Connect, error) {
	//获取配置信息
	setting := redisSetting{
		Server: "127.0.0.1:6379", Password: "", Database: "",
		Idle: 30, Active: 100, Timeout: 240,
	}

	if vv, ok := config.Setting["server"].(string); ok && vv != "" {
		setting.Server = vv
	}
	if vv, ok := config.Setting["password"].(string); ok && vv != "" {
		setting.Password = vv
	}

	//数据库，redis的0-16号
	if v, ok := config.Setting["database"].(string); ok {
		setting.Database = v
	}

	if vv, ok := config.Setting["idle"].(int64); ok && vv > 0 {
		setting.Idle = int(vv)
	}
	if vv, ok := config.Setting["active"].(int64); ok && vv > 0 {
		setting.Active = int(vv)
	}
	if vv, ok := config.Setting["timeout"].(int64); ok && vv > 0 {
		setting.Timeout = time.Second * time.Duration(vv)
	}
	if vv, ok := config.Setting["timeout"].(string); ok && vv != "" {
		td, err := util.ParseDuration(vv)
		if err == nil {
			setting.Timeout = td
		}
	}

	return &redisConnect{
		name: name, config: config, setting: setting,
		events: make(map[string]string, 0),
	}, nil
}

//打开连接
func (connect *redisConnect) Open() error {
	connect.client = &redis.Pool{
		MaxIdle: connect.setting.Idle, MaxActive: connect.setting.Active, IdleTimeout: connect.setting.Timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", connect.setting.Server)
			if err != nil {
				log.Warning("event.redis.dial", err)
				return nil, err
			}

			//如果有验证
			if connect.setting.Password != "" {
				if _, err := c.Do("AUTH", connect.setting.Password); err != nil {
					c.Close()
					log.Warning("event.redis.auth", err)
					return nil, err
				}
			}
			//如果指定库
			if connect.setting.Database != "" {
				if _, err := c.Do("SELECT", connect.setting.Database); err != nil {
					c.Close()
					log.Warning("event.redis.select", err)
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	//打开一个试一下
	conn := connect.client.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	return nil
}

func (connect *redisConnect) Health() (event.Health, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return event.Health{Workload: connect.actives}, nil
}

//关闭连接
func (connect *redisConnect) Close() error {
	if connect.client != nil {

		//发送退出消息
		for _, stoper := range connect.events {
			if stoper != "" {
				connect.Publish(stoper, nil)
			}
		}

		if err := connect.client.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (connect *redisConnect) Accept(delegate event.Delegate) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	//绑定
	connect.delegate = delegate

	return nil
}

//省点事，一起注册了
//除了单机和nats，不打算支持其它总线驱动了
//以后要支持其它总线驱动的时候，再说
//还有种可能，就是，nats中事件单独定义使用jetstream做持久的时候
//那也可以同一个Register方法，定义实体来注册，加入Type或其它方式来区分
func (connect *redisConnect) Register(group, name string) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	connect.events[name] = ""

	return nil
}

//开始订阅者
func (connect *redisConnect) Start() error {
	if connect.running {
		return errAlreadyRunning
	}

	//这个循环，用来从redis读消息
	for name, _ := range connect.events {
		//用来接受退出消息的，但是这样会有问题
		stoper := chef.Generate(name)
		connect.events[name] = stoper

		go func() {
			conn := connect.client.Get()
			defer conn.Close()

			psc := redis.PubSubConn{Conn: conn}
			psc.Subscribe(name, stoper) //一次订阅多个
			defer psc.Close()

			for {
				switch rec := psc.Receive().(type) {
				case redis.Message:
					if rec.Channel == stoper {
						// 不是自己，就是退出信号
						// 因为是在循环等待，所以上自动等到上一个事件执行完成，才会到这里
						// 但是只在单线程事件下有效，比如某个事件开了线程池，同时可以跑几个
						// 但是没有跑满的时候， 就直接退出了， 没法完成正在执行的任务
						// 这是个BUG，

						// 待优化
						//这里需要加一个等待线程结束的处理，或者直接把池，放在这里

						return
					} else {
						connect.delegate.Serve(name, rec.Data)
					}
				case redis.Subscription:
				case error:
					//不管，继续循环
					// break
				}

				bytes, _ := redis.ByteSlices(conn.Do("BRPOP", name, 3))
				if bytes != nil && len(bytes) >= 2 {
					channel := string(bytes[0])
					data := bytes[1]

					if channel == name {
						connect.delegate.Serve(name, data)
					} else {
					}
				}
			}
		}()

	}

	connect.running = true
	return nil
}

func (connect *redisConnect) Publish(name string, data []byte) error {
	if connect.client == nil {
		return errInvalidConnection
	}

	conn := connect.client.Get()
	defer conn.Close()

	//写入
	_, err := conn.Do("PUBLISH", name, data)

	if err != nil {
		log.Warning("event.redis.publish", err)
		return err
	}

	return nil
}
