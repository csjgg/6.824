## Raft

首先要基于[论文](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)

对于leader：

- Start方法，负责将命令写入logs，同时通过条件变量唤醒负责向follower发送AppendEntries rpc的线程
- 选举成功时，对每一个follower，发起一个线程，在一个循环中，利用条件变量，不断的check和发送apply appends rpc。循环在不是leader时被打破，线程也会自动退出（对leader的所有线程都是）。
- 选举成功时，发起一个线程，不断发送heart beat。
- 选举成功时，发起一个线程，不断检查logs复制的情况和设置commitindex。

对于candidate：

- 候选人对每一个peer发起投票请求，并利用条件变量和超时唤醒进行检查。

对于follower（对于所有的peer）：

- 初始化时发起一个线程，用于检查commitindex和lastapplied index，并向applych发送commit的logs信息。
- 初始化时发起一个线程，用于检查heartbeat时间并在超时时转换成candidate

### 易错问题

- logs的index从1开始，更利于维护
- 如果在rpc的过程中见到了比自己term大的，就同步到更大的term，并转换为follower，但是不重置heartbeat time
- 重置heartbeat的时间只有三处：当转换成candidate时，当收到投票请求并投赞成票时，当收到合法的AppendEntries rpc时
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