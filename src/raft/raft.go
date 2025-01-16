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
	"context"
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

type RaftState uint32

const (
	RaftStateFollower RaftState = iota
	RaftStateCandidate
	RaftStateLeader
)

func (rs RaftState) String() string {
	switch rs {
	case RaftStateFollower:
		return "Follower"
	case RaftStateCandidate:
		return "Candidate"
	case RaftStateLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	term     int64
	state    RaftState
	votedFor int32

	lastReceivedHeartBeat time.Time
	electionTimeout       time.Duration
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {
	stateAddr := (*uint32)(&rf.state)
	state := atomic.LoadUint32(stateAddr)
	return int(atomic.LoadInt64(&rf.term)), state == uint32(RaftStateLeader)
}

func (rf *Raft) getTerm() int64 {
	return atomic.LoadInt64(&rf.term)
}

func (rf *Raft) getState() RaftState {
	stateAddr := (*uint32)(&rf.state)
	state := atomic.LoadUint32(stateAddr)
	return RaftState(state)
}

func (rf *Raft) getVotedFor() int32 {
	return atomic.LoadInt32(&rf.votedFor)
}

func (rf *Raft) setTerm(newTerm int64) {
	atomic.StoreInt64(&rf.term, newTerm)
}

func (rf *Raft) setState(newState RaftState) {
	stateAddr := (*uint32)(&rf.state)
	atomic.StoreUint32(stateAddr, uint32(newState))
}

func (rf *Raft) setVotedFor(newVotedFor int32) {
	atomic.StoreInt32(&rf.votedFor, newVotedFor)
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

type AppendEntriesArgs struct {
	Term     int64
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("node: %d, received heartbeat from peer: %d", rf.me, args.LeaderId)
	if args.Term < rf.getTerm() {
		reply.Term = rf.getTerm()
		reply.Success = false
		return
	}
	rf.becomeFollower()
	rf.mu.Lock()
	rf.lastReceivedHeartBeat = time.Now()
	rf.mu.Unlock()
	reply.Term = args.Term
	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int64
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	currentTerm := rf.getTerm()
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}

	if currentTerm < args.Term {
		rf.setTerm(args.Term)
		rf.becomeFollower()
	}
	if rf.getVotedFor() == -1 {
		rf.setVotedFor(int32(args.CandidateId))
		reply.Term = args.Term
		rf.setTerm(args.Term)
		reply.VoteGranted = true
		return
	}

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

func (rf *Raft) becomeCandidate() {
	rf.setState(RaftStateCandidate)
	rf.setTerm(rf.getTerm() + 1)
	rf.mu.Lock()
	rf.lastReceivedHeartBeat = time.Now()
	rf.mu.Unlock()
	rf.setVotedFor(int32(rf.me))
	rf.restartElectionTimeout()
}

func (rf *Raft) becomeLeader() {
	DPrintf("ELECTION: peerId: %d becoming leader", rf.me)
	rf.setState(RaftStateLeader)
	rf.setVotedFor(-1)
	go rf.sendHeartbeat()
}

func (rf *Raft) becomeFollower() {
	DPrintf("STATE: peerId: %d becoming follower", rf.me)
	rf.setState(RaftStateFollower)
	rf.setVotedFor(-1)
	rf.restartElectionTimeout()

}

func (rf *Raft) sendHeartbeat() {
	if rf.getState() != RaftStateLeader {
		return
	}
	for peerId := range rf.peers {
		if peerId != rf.me {
			go func() {
				currentTerm := rf.getTerm()
				args := AppendEntriesArgs{LeaderId: rf.me, Term: currentTerm}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(peerId, &args, &reply)
				if reply.Term > currentTerm {
					rf.setTerm(reply.Term)
					rf.becomeFollower()
				}
			}()
		}
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		if rf.getState() == RaftStateLeader {
			go rf.sendHeartbeat()
		}
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}

func (rf *Raft) restartElectionTimeout() {
	rf.mu.Lock()
	rf.electionTimeout = time.Duration(160+(rand.Int63()%200)) * time.Millisecond
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		rf.mu.Lock()
		timeout := rf.electionTimeout
		rf.mu.Unlock()
		time.Sleep(timeout)
		rf.mu.Lock()
		lastReceivedHeartBeat := rf.lastReceivedHeartBeat
		rf.mu.Unlock()
		if rf.getState() != RaftStateLeader {
			DPrintf("peerId: %d, last elapsed: %v, limit: %v", rf.me, time.Since(lastReceivedHeartBeat), timeout)
		}

		shouldStartElection := time.Since(lastReceivedHeartBeat) > timeout && rf.getState() != RaftStateLeader
		if shouldStartElection {
			go rf.startElection()
		}
	}
}

func (rf *Raft) collectVote(peerId int, ctx context.Context, resultCh chan<- RequestVoteReply) {
	args := RequestVoteArgs{Term: rf.getTerm(), CandidateId: rf.me}
	reply := RequestVoteReply{}
	rf.sendRequestVote(peerId, &args, &reply)
	select {
	case resultCh <- reply:
		DPrintf("Result from election for candidate: %d, from peer: %d result: %v", rf.me, peerId, reply)
		return
	case <-ctx.Done():
		return
	}
}

func (rf *Raft) startElection() {
	DPrintf("ELECTION: Starting election from node: %d", rf.me)
	rf.becomeCandidate()
	votes := 1
	necessaryVotes := (len(rf.peers) / 2) + 1
	resultsChan := make(chan RequestVoteReply)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	var wg sync.WaitGroup
	defer cancel()
	for peerId := range rf.peers {
		if peerId != rf.me {
			wg.Add(1)
			go rf.collectVote(peerId, ctx, resultsChan)
		}
	}
	go func() {
		wg.Wait()
		close(resultsChan)
	}()
	for reply := range resultsChan {
		if reply.Term == rf.getTerm() && reply.VoteGranted {
			votes++
		}
		if votes >= necessaryVotes {
			rf.becomeLeader()
			break
		}
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
	rf := &Raft{state: RaftStateFollower, term: 0, votedFor: -1}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.restartElectionTimeout()
	go rf.ticker()
	go rf.heartbeat()

	return rf
}
