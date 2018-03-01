mns
===

worker: FBP


`config`


```hocon
include "secret.conf"

components.mns.endpoint {

	access-key-id = ${aliyun.access-key-id}
	access-key-secret = ${aliyun.access-key-secret}
	endpoint = ${aliyun.endpoint}

	# listen queues, receive message and send to mailbox
	queues {
		todo-task-new {}
		todo-task-get {}
	}
}

```



`secret.conf`

```hocon
aliyun {
	access-key-id = ""
	access-key-secret = ""
	endpoint = "https://xxx.mns.cn-beijing.aliyuncs.com/"
}
```