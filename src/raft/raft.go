package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A log entry.
type Log struct {
	Term    int
	Command interface{}
}

// State
const Follower = 1
const Candidate = 2
const Leader = 3

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leaderid int
	state    int32
	applyCh  chan ApplyMsg

	// Persistent state on all servers
	currentTerm   int
	votedFor      int // -1 if none
	voted         int
	heartbeattime time.Time

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int //index of highest log entry known to be replicated on server

	// logs
	logs          []Log
	getlog        sync.Cond
	apply         sync.Cond
	snapshotindex int
}

// func used to check and set state
func (rf *Raft) isleader() bool {
	return atomic.LoadInt32(&rf.state) == Leader
}
func (rf *Raft) iscandidate() bool {
	return atomic.LoadInt32(&rf.state) == Candidate
}
func (rf *Raft) setfollower() {
	atomic.StoreInt32(&rf.state, Follower)
}
func (rf *Raft) setcandidate() {
	atomic.StoreInt32(&rf.state, Candidate)
}
func (rf *Raft) setleader() {
	atomic.StoreInt32(&rf.state, Leader)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.isleader()
	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastApplied)
	e.Encode(rf.commitIndex)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	rf.persister.Save(data, nil)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	var lastApplied int
	var commitindex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&lastApplied) != nil || d.Decode(&commitindex) != nil || d.Decode(&logs) != nil {
		log.Printf("Persister is not enough")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastApplied = lastApplied
		rf.commitIndex = commitindex
		rf.logs = logs
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if index < rf.snapshotindex || index > (len(rf.logs)-1+rf.snapshotindex) {
			return
		}
		DPrintf("Raft %v Save to snapshot to index %v", rf.me, index)
		rf.persister.Save(nil, snapshot)
		// last log at index0
		rf.logs = rf.logs[index-rf.snapshotindex:]
		rf.snapshotindex = index
		// DPrintf("Raft %v logs %v", rf.me, rf.logs)
	}()
}

// AppendEntries RPC args structure.
type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log //log entries to store
	LeaderCommit int   //leader’s commitIndex
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int  // current term, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	Xterm   int
	Xindex  int
	Xlen    int
}

// send apply message to service
func (rf *Raft) Sendapplymessage() {
	go func() {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		rf.apply.Signal()
		rf.mu.Unlock()
	}()

	rf.mu.Lock()
	for !rf.killed() {
		for rf.lastApplied < rf.commitIndex {
			commitindex := rf.commitIndex
			for i := rf.lastApplied + 1; i <= commitindex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i-rf.snapshotindex].Command,
					CommandIndex: i,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
			if rf.lastApplied < commitindex {
				rf.lastApplied = commitindex
			}
			rf.persist()
		}
		rf.apply.Wait()
	}
	rf.mu.Unlock()
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		rf.leaderid = args.LeaderId
		rf.currentTerm = args.Term
		rf.setfollower()
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > (len(rf.logs) - 1 + rf.snapshotindex) {
				rf.commitIndex = len(rf.logs) - 1 + rf.snapshotindex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			DPrintf("Raft %v commitindex %v", rf.me, rf.commitIndex)
			rf.apply.Signal()
		}
		rf.persist()
		if len(args.Entries) == 0 && args.PrevLogIndex == 0 && args.PrevLogTerm == 0 {
			// DPrintf("Raft %v receive heartbeat from %v, term %v", rf.me, args.LeaderId, args.Term)
			// heartbeat
			rf.heartbeattime = time.Now()
			reply.Success = true
			reply.Term = args.Term
			rf.persist()
			return
		}
		// follower's log is too short
		if len(rf.logs)-1+rf.snapshotindex < args.PrevLogIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.persist()
			reply.Xlen = len(rf.logs) + rf.snapshotindex
			return
		}
		// return Xterm
		if rf.logs[args.PrevLogIndex-rf.snapshotindex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.persist()
			reply.Xterm = rf.logs[args.PrevLogIndex-rf.snapshotindex].Term
			previndex := args.PrevLogIndex
			for previndex-rf.snapshotindex > 0 && rf.logs[previndex-rf.snapshotindex].Term != args.PrevLogTerm {
				previndex--
			}
			reply.Xindex = previndex + 1
			return
		}
		rf.heartbeattime = time.Now()
		if len(args.Entries) == 0 {
			reply.Success = true
			reply.Term = args.Term
			rf.persist()
			return
		}
		DPrintf("Raft %v receive AppendEntries from %v, term %v,%v, entry len %v", rf.me, args.LeaderId, args.Term, rf.currentTerm, len(args.Entries))
		// append entries
		rf.logs = rf.logs[:args.PrevLogIndex+1-rf.snapshotindex]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
		reply.Success = true
		reply.Term = args.Term
		// DPrintf("Raft %v logs %v", rf.me, rf.logs)
		return
	}
	DPrintf("Raft %v reject AppendEntries from %v, term %v,%v", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	reply.Success = false
	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if !rf.isleader() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// leader: send heartbeat to all followers in time
func (rf *Raft) sendHeartbeat() {
	for !rf.killed() {
		if !rf.isleader() {
			// if other win, stop heartbeat
			break
		}
		// send heartbeat to all followers
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			if rf.commitIndex > rf.matchIndex[i] {
				args.LeaderCommit = rf.matchIndex[i]
			} else {
				args.LeaderCommit = rf.commitIndex
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
		ms := 100 + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// copy log to follower
func (rf *Raft) Copytofollower(i int) {
	// used for wake this thread
	go func() {
		for rf.isleader() && !rf.killed() {
			time.Sleep(20 * time.Millisecond)
			rf.mu.Lock()
			rf.getlog.Signal()
			rf.mu.Unlock()
		}
	}()

	// check and copy to follower
	rf.mu.Lock()
	for rf.isleader() && !rf.killed() {
		for rf.matchIndex[i] < len(rf.logs)-1+rf.snapshotindex {
			// DPrintf("Raft %v matchindex[%v] %v nextindex[%v] %v", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
			if !rf.isleader() {
				break
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[i]-1-rf.snapshotindex].Term,
				Entries:      rf.logs[rf.nextIndex[i]-rf.snapshotindex:],
			}
			if rf.commitIndex > rf.matchIndex[i] {
				args.LeaderCommit = rf.matchIndex[i]
			} else {
				args.LeaderCommit = rf.commitIndex
			}
			nextindex := rf.nextIndex[i]
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(i, &args, &reply)
			rf.mu.Lock()

			// check result
			if reply.Success {
				if rf.nextIndex[i] < nextindex+len(args.Entries) {
					rf.nextIndex[i] = nextindex + len(args.Entries)
					rf.matchIndex[i] = nextindex + len(args.Entries) - 1
					DPrintf("Raft %v matchindex[%v] %v", rf.me, i, rf.matchIndex[i])
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.setfollower()
					rf.votedFor = -1
					rf.leaderid = -1
					rf.persist()
					break
				}
				if ok {
					if reply.Xterm != 0 {
						nextindex--
						for nextindex-rf.snapshotindex > 0 && rf.logs[nextindex-rf.snapshotindex].Term > reply.Xterm {
							nextindex--
						}
						if nextindex-rf.snapshotindex == 0 || rf.logs[nextindex-rf.snapshotindex].Term < reply.Term {
							nextindex = reply.Xindex
						}
					} else {
						nextindex = reply.Xlen
					}
					if nextindex < rf.nextIndex[i] {
						rf.nextIndex[i] = nextindex
					}
				}
			}
		}
		rf.getlog.Wait()
	}
	rf.mu.Unlock()
}

// check if the log is committed
// if majority of followers have the log, commit it
// the heartbeats will update commitIndex, so we no need to send rpc to followers
func (rf *Raft) Checkcommit() {
	DPrintf("Raft %v check commit", rf.me)
	for !rf.killed() && rf.isleader() {
		rf.mu.Lock()
		for i := (len(rf.logs) - 1 + rf.snapshotindex); i > rf.commitIndex && rf.logs[i-rf.snapshotindex].Term == rf.currentTerm; i-- {
			if !rf.isleader() {
				break
			}
			count := 1
			for j := range rf.peers {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= i {
					count++
				}
			}
			if count > (len(rf.peers) >> 1) {
				// send apply message
				rf.commitIndex = i
				rf.persist()
				rf.apply.Signal()
				DPrintf("Raft %v commit to log %v cmd %v, snapshotindex %v, commitIndex %v, i %v,len %v", rf.me, i, rf.logs[i-rf.snapshotindex].Command, rf.snapshotindex, rf.commitIndex, i, len(rf.logs)-1)
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.setfollower()
		rf.persist()
	} else if rf.currentTerm > args.Term {
		DPrintf("Raft %v reject vote request from %v, term %v, because of term is small", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
			(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= (len(rf.logs)-1+rf.snapshotindex))) {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.heartbeattime = time.Now()
		rf.persist()
		return
	}
	DPrintf("Raft %v reject vote request from %v, term %v, because of he is voted or log is small", rf.me, args.CandidateId, args.Term)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// use as go routine
// send vote request, check the result
// if get majority vote, wake the main thread
func (rf *Raft) SendRV(i int, condi *sync.Cond) {
	if !rf.iscandidate() {
		return
	}
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1 + rf.snapshotindex,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	reply := RequestVoteReply{}
	rf.mu.Unlock()
	rf.sendRequestVote(i, &args, &reply)
	rf.mu.Lock()
	if reply.VoteGranted {
		rf.voted++
		DPrintf("Raft %v get vote from %v, voted %d", rf.me, i, rf.voted)
		if rf.voted > len(rf.peers)/2 {
			condi.Signal()
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.setfollower()
			rf.votedFor = -1
			rf.leaderid = -1
			condi.Signal()
			rf.persist()
		}
		DPrintf("Raft %v get reject from %v", rf.me, i)
	}
	rf.mu.Unlock()
}

// start election
// send RequestVote to all other servers
// use condition variable to wait for the result or it time out
func (rf *Raft) Startelection() {
	rf.voted = 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.heartbeattime = time.Now()
	rf.setcandidate()
	rf.persist()

	// condition variable to wait for the result
	var condi = sync.NewCond(&rf.mu)

	// send RequestVote to all other servers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.SendRV(i, condi)
	}
	go func() {
		time.Sleep(time.Duration(300+(rand.Int63()%1500)) * time.Millisecond)
		rf.mu.Lock()
		condi.Signal()
		rf.mu.Unlock()
	}()

	condi.Wait()

	// check the result
	if rf.voted > len(rf.peers)/2 {
		if rf.iscandidate() {
			DPrintf("Raft %v become leader", rf.me)
			rf.leaderid = rf.me
			rf.setleader()
			// initialize nextIndex and matchIndex
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.logs) + rf.snapshotindex
				rf.matchIndex[i] = 0
			}
			go rf.sendHeartbeat()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.Copytofollower(i)
			}
			go rf.Checkcommit()
		}
	} else {
		rf.setfollower()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := false
	if !rf.isleader() || rf.killed() {
		return index, term, isLeader
	}
	index = len(rf.logs) + rf.snapshotindex
	term = rf.currentTerm
	isLeader = true
	rf.logs = append(rf.logs, Log{
		Term:    rf.currentTerm,
		Command: command,
	})
	// DPrintf("Raft %v logs %v", rf.me, rf.logs)
	rf.getlog.Broadcast()
	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		if !rf.isleader() {
			rf.mu.Lock()
			for !rf.isleader() && time.Since(rf.heartbeattime) > (time.Duration(500+(rand.Int63()%1500))*time.Millisecond) {
				DPrintf("Raft %v start election, term %v", rf.me, rf.currentTerm+1)
				rf.Startelection()
			}
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (3A, 3B, 3C).
	rf.leaderid = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeattime = time.Now()
	rf.setfollower()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.logs = make([]Log, 1)
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.snapshotindex = 0
	rf.getlog = *sync.NewCond(&rf.mu)
	rf.apply = *sync.NewCond(&rf.mu)
	DPrintf("Raft %v created", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start send apply message
	go rf.Sendapplymessage()

	return rf
}
