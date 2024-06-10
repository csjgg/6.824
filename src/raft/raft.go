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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

// Raft server state.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

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
	state    State
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
	logs []Log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
}

// send apply message to service
func (rf *Raft) Sendapplymessage(endindex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= endindex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
	}
	rf.lastApplied = endindex
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeattime = time.Now()
	if args.Term >= rf.currentTerm {
		rf.leaderid = args.LeaderId
		rf.currentTerm = args.Term
		rf.state = Follower
		if args.LeaderCommit > rf.commitIndex {
			if len(rf.logs)-1 > args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.logs) - 1
			}
			go rf.Sendapplymessage(rf.commitIndex)
		}
		if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			DPrintf("Raft %v reject AppendEntries from %v, term %v,%v", rf.me, args.LeaderId, args.Term, rf.currentTerm)
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
		if len(args.Entries) == 0 {
			// DPrintf("Raft %v receive heartbeat from %v, term %v", rf.me, args.LeaderId, args.Term)
			// heartbeat
			reply.Success = true
			reply.Term = args.Term
			return
		}
		DPrintf("Raft %v receive AppendEntries from %v, term %v,%v, entry len %v", rf.me, args.LeaderId, args.Term, rf.currentTerm, len(args.Entries))
		// append entries
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		reply.Success = true
		reply.Term = args.Term
		DPrintf("Raft %v logs %v", rf.me, rf.logs)
		return
	}
	DPrintf("Raft %v reject AppendEntries from %v, term %v,%v", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	reply.Success = false
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// leader: send heartbeat to all followers in time
func (rf *Raft) sendHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			// if other win, stop heartbeat
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
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
		ms := 10 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// copy log to follower
func (rf *Raft) Copytofollower(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.state == Leader && rf.matchIndex[i] < len(rf.logs)-1 && !rf.killed() {
		if rf.nextIndex[i] <= 1 {
			DPrintf("Raft %v nextIndex %v, matchIndex %v, log len %v", rf.me, rf.nextIndex[i], rf.matchIndex[i], len(rf.logs))
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
			Entries:      rf.logs[rf.nextIndex[i]:],
		}
		if rf.commitIndex > rf.matchIndex[i] {
			args.LeaderCommit = rf.matchIndex[i]
		} else {
			args.LeaderCommit = rf.commitIndex
		}
		successnextindex := rf.nextIndex[i] + len(args.Entries)
		successmatchindex := successnextindex - 1
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		ok := rf.sendAppendEntries(i, &args, &reply)
		rf.mu.Lock()
		if reply.Success {
			rf.matchIndex[i] = successmatchindex
			rf.nextIndex[i] = successnextindex
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.leaderid = -1
				return
			}
			if ok {
				rf.nextIndex[i]--
			}
		}
	}
}

// check if the log is committed
// if majority of followers have the log, commit it
// the heartbeats will update commitIndex, so we no need to send rpc to followers
func (rf *Raft) Checkcommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft %v check commit", rf.me)
	for rf.state == Leader && !rf.killed() && rf.commitIndex < len(rf.logs)-1 {
		for i := len(rf.logs) - 1; i > rf.commitIndex && rf.logs[i].Term == rf.currentTerm; i-- {
			count := 1
			for j := range rf.peers {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= i {
					count++
				}
			}
			if count > len(rf.peers)>>1 {
				// send apply message
				rf.commitIndex = i
				go rf.Sendapplymessage(i)
				DPrintf("Raft %v commit to log %v cmd %v, lastapplied %v, term %v", rf.me, i, rf.logs[i].Command, rf.lastApplied, rf.logs[i].Term)
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
	}
}

// start agreement
// add to own log, then send AppendEntries to all followers
func (rf *Raft) Startagreement() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft %v logs %v", rf.me, rf.logs)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.Copytofollower(i)
	}
	go rf.Checkcommit()
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
	rf.heartbeattime = time.Now()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	} else if rf.currentTerm > args.Term {
		DPrintf("Raft %v reject vote request from %v, term %v, because of term is small", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
			(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1)) {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("Raft %v reject vote request from %v, term %v, because of he is voted or log is small", rf.me, args.CandidateId, args.Term)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
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
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
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
			condi.Broadcast()
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.leaderid = -1
			condi.Broadcast()
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
	rf.state = Candidate

	// condition variable to wait for the result
	var condi = sync.NewCond(&rf.mu)

	// send RequestVote to all other servers
	for i := range rf.peers {
		if rf.state != Candidate {
			// heartbeat received
			return
		}
		if rf.voted > len(rf.peers)/2 {
			break
		}
		if i == rf.me {
			continue
		}
		go rf.SendRV(i, condi)
	}
	go func() {
		time.Sleep(time.Duration(100+(rand.Int63()%100)) * time.Millisecond)
		rf.mu.Lock()
		condi.Broadcast()
		rf.mu.Unlock()
	}()

	condi.Wait()

	// check the result
	if rf.voted > len(rf.peers)/2 {
		if rf.state == Candidate {
			DPrintf("Raft %v become leader", rf.me)
			rf.leaderid = rf.me
			rf.state = Leader
			// initialize nextIndex and matchIndex
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}
			go rf.sendHeartbeat()
		}
	} else {
		rf.state = Follower
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
	if rf.state != Leader || rf.killed() {
		return index, term, isLeader
	}
	go rf.Startagreement()
	// Your code here (3B).
	index = len(rf.logs)
	term = rf.currentTerm
	isLeader = true
	rf.logs = append(rf.logs, Log{
		Term:    rf.currentTerm,
		Command: command,
	})

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
		rf.mu.Lock()
		for rf.state != Leader && time.Since(rf.heartbeattime) > (time.Duration(700+(rand.Int63()%700))*time.Millisecond) {
			DPrintf("Raft %v start election, term %v", rf.me, rf.currentTerm)
			rf.Startelection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
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
	rf.state = Follower
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.logs = make([]Log, 1)
	DPrintf("Raft %v created", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
