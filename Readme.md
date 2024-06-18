## Raft (Lab 3)

首先要基于[论文](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)

对于leader：

- Start方法，负责将命令写入logs，同时通过条件变量唤醒负责向follower发送AppendEntries rpc的协程（copy to follower）
- 选举成功时，对每一个follower，发起一个（copy to follower）协程，在一个循环中，利用条件变量，不断的check和发送apply appends rpc。循环在不是leader时被打破，协程也会自动退出（对leader的所有协程都是）。（当网络原因导致rpc发送失败时，会发起更多协程(<=3)尝试）
- 选举成功时，发起一个协程，负责定期对每一个follower发起heartbeat。
- 当发送AppendEntries rpc前，需要检查nextindex和snapshotindex，如果nextindex更小，就需要Installsnapshot rpc
- 选举成功时，发起一个协程，不断检查logs复制的情况和设置commitindex。

对于candidate：

- 候选人对每一个peer发起投票请求，并利用条件变量和超时唤醒进行检查。

对于follower（对于所有的peer）：

- 初始化时发起一个协程，用于检查commitindex和lastapplied index，并向applych发送commit的logs信息。
- 初始化时发起一个协程，用于检查heartbeat时间并在超时时转换成candidate

### 易错问题

- logs的index从1开始，更利于维护，引入snapshot后，index 0的位置可以放snapshot的最后一个log。
- 如果在rpc的过程中见到了比自己term大的，就同步到更大的term，并转换为follower，但是不重置heartbeat time
- 重置heartbeat的时间只有四处：当转换成candidate时，当收到投票请求并投赞成票时，当收到合法的AppendEntries rpc时，当收到合法的snapshot时。
- leader不能check并commit比自己当前任期小的logs，原因见论文。
- heartbeat超时时间通常是发送heartbeat时间的3～5倍，同时要保证时间的随机性。
- 3C需要实现当append entries失败时更快速的更新nextindex才能比较稳定的pass，方式如下

One possibility is to have a rejection message include:

> ​    XTerm:  term in the conflicting entry (if any)
>
> ​    XIndex: index of first entry with that term (if any)
>
> ​    XLen:   log length

Then the leader's logic can be something like:

>   Case 1: leader doesn't have XTerm:
>
> ​    nextIndex = XIndex
>
>   Case 2: leader has XTerm:
>
> ​    nextIndex = leader's last entry for XTerm
>
>   Case 3: follower's log is too short:
>
> ​    nextIndex = XLen

- heartbeat除了不包含logs之外，其他的信息都需要包含，同时也需要受到reply之后处理nextindex等信息，同时也需要处理nextindex过小所以要先发送snapshot的问题。这意味着需要等待heartbeat的执行完毕，但是这可能会消耗超过要求发送heartbeat的时间，所以需要单独抽象成协程，而一到应该发heartbeat时就发起这样的协程。
- 在对所有follower发送rpc时，不能在一个循环中每次都使用rf.currentTerm填充args，而应该在循环前在lock之下处理好args，然后在循环中发送。
- 在AppendEntries rpc中，需要比对args中携带的logs和follower的logs，当遇到不同的log时，从此截断，并拼接args中的剩余日志。（主要目的是防止过时的rpc将已经复制好的log截断）

End：

> 通过还是比较稳的。时间在九分钟左右，个人觉得hints中说的6分钟是不合理的，因为往年全写的6分钟，而今年的测试明显增加了且变难了，可能是忘记了修改hints。暂时不修改了，如果写后面lab时出现了问题再修改吧。

