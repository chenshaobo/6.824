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

import "sync"
import (
	"github.com/chenshaobo/6.824/labrpc"
	"time"
	"math/rand"
	"encoding/gob"
	"bytes"
	"runtime"
)

// import "bytes"
// import "encoding/gob"


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
const (
	RaftRoleFollowers = iota
	RaftRoleCandidates
	RaftRoleLeaders
)

type raftConfig struct {
	hearbeatTimeout       time.Duration
	leaderHearbeatTimeOut time.Duration
}
type Raft struct {
	mu              sync.Mutex
	peers           []*labrpc.ClientEnd
	persister       *Persister
	me              int         // index into peers[]

                                // Your data here.
                                // Look at the paper's Figure 2 for a description of what
                                // state a Raft server must maintain.
	config          raftConfig  // some config
	currentTerm     int         //currentTerm  latest term server has seen (initialized to 0 on first boot, increases monotonically
	currentIndex    int

	lastCommitIndex int
	lastApplied     int
	nextIndexs      []int       //The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower.
	matchIndexs     []int

	raftLogs        []RaftEntry //log entries;each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	role            int

	heartbeat       chan bool
	timeoutTimer    *time.Timer
	applyCh         chan ApplyMsg
	commitCh        chan bool
	voteCh          chan int

	votes           int
	voteFor         int         // vote who
}

type RaftEntry struct {
	Index   int
	Command interface{}
	Term    int
}

type RaftState struct {
	Term int `json:"term"`
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
	return rf.currentTerm, rf.role == RaftRoleLeaders
}

func (rf *Raft) getLogLastIndex() int {
	return rf.raftLogs[len(rf.raftLogs) - 1].Index
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.raftLogs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.raftLogs)
}

func (rf *Raft) restTimer() {
	var timeout time.Duration
	switch (rf.role) {
	case RaftRoleFollowers:
		timeout = rf.config.hearbeatTimeout
	case RaftRoleLeaders:
		timeout = rf.config.leaderHearbeatTimeOut
	case RaftRoleCandidates:
		randomNum := rand.Int63n(1000)
		rand.Seed(randomNum)
		timeout = time.Millisecond * time.Duration(randomNum + 500)
	}
	rf.timeoutTimer.Reset(timeout)
	//DPrintf("[REST TIMER]FINISH RESET heart TIMER:%v,timeout:%v,restult:%v,now:%v\n", rf.me, timeout, r, time.Now())
}



//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Candidate    int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term    int
	Votefor int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Votefor = rf.voteFor
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ChangeRaftRole(RaftRoleFollowers)
		rf.voteFor = -1
	}

	reply.Term = rf.currentTerm
	lastIndex := rf.getLogLastIndex()
	lastTerm := rf.raftLogs[len(rf.raftLogs) - 1].Term

	uptoDate := false
	if (args.LastLogTerm > lastTerm) || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
		uptoDate = true
	}

	if (rf.voteFor == -1 || rf.voteFor == args.Candidate) && uptoDate {
		reply.Votefor = args.Candidate
		rf.voteFor = args.Candidate
		//rf.heartbeat <- true
	}

	//DPrintf("[Request Vote] %v receive requestVote,old term:%v,%v's term :%v,%v votefor :%v\n", rf.me, reply.Term, rf.me, rf.currentTerm, args, rf.voteFor)
	reply.Votefor = rf.voteFor

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if reply.Votefor == rf.me {
		rf.voteCh <- server
	}
	//DPrintf("[Request Vote] %v request [%v] vote for :%v\n", rf.me, server, reply.Votefor)
	return ok
}

type AppendEntries struct {
	Term              int
	LeaderID          int
	LeaderCommitIndex int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []RaftEntry
}

type AppendEntriesReply struct {
	MyTerm    int
	Success   bool
	NextIndex int
}
//
func (rf *Raft) AppendEntriesRPC(args AppendEntries, reply *AppendEntriesReply) {
	//reset the hearbeat time out
	//rf.restTimer(rf.role)
	rf.heartbeat <- true
	if len(args.Entries) > 0 {
		DPrintf("[RECEIVE APPEND]%v Receive %v's appendentries ,role is :%v,rf.term:%v,args%v\n", rf.me, args.LeaderID, rf.role, rf.currentTerm, args)
	}
	reply.MyTerm = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term >= rf.currentTerm {
		switch rf.role {
		case RaftRoleLeaders:
		case RaftRoleCandidates:
			rf.currentTerm = args.Term
			rf.ChangeRaftRole(RaftRoleFollowers)
		case RaftRoleFollowers:
			rf.currentTerm = args.Term
		}
	}

	if args.PrevLogIndex > rf.getLogLastIndex() {
		reply.NextIndex = rf.currentIndex + 1
		return
	}

	if rf.raftLogs[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	baseIndex := rf.raftLogs[0].Index
	if args.PrevLogIndex > baseIndex {
		term := rf.raftLogs[args.PrevLogIndex - baseIndex].Term
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.raftLogs[i - baseIndex].Term != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}

	if len(args.Entries) > 0 {
		if args.PrevLogIndex >= baseIndex {
			rf.raftLogs = rf.raftLogs[:args.PrevLogIndex + 1 - baseIndex]
			rf.raftLogs = append(rf.raftLogs, args.Entries...)
			reply.NextIndex = rf.getLogLastIndex() + 1
		}
	}

	if args.LeaderCommitIndex > rf.lastCommitIndex {
		lastIndex := rf.getLogLastIndex()
		if args.LeaderCommitIndex >= lastIndex {
			rf.lastCommitIndex = lastIndex
		} else {
			rf.lastCommitIndex = args.LeaderCommitIndex
		}
		rf.commitCh <- true
	}

	reply.Success = true
}

//
func (rf *Raft) sendAppendEntries(server int, args AppendEntries, reply *AppendEntriesReply) bool {

	//DPrintf("[sendAppendEntries] %v send %v to %v\n",rf.me,args,server)
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	if ok {
		if rf.role != RaftRoleLeaders {
			return ok
		}

		if rf.currentTerm != args.Term {
			return ok
		}
		if reply.MyTerm > rf.currentTerm {
			rf.currentTerm = reply.MyTerm
			rf.ChangeRaftRole(RaftRoleFollowers)
			rf.persist()
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndexs[server] = args.Entries[len(args.Entries) - 1].Index + 1
				rf.matchIndexs[server] = rf.nextIndexs[server] - 1
			}
		}
		DPrintf("[sendAppendEntries]%v send hearbeat to :%v,return term:%v\n", rf.me, server, reply.MyTerm)
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	if rf.role == RaftRoleLeaders {
		rf.mu.Lock()
		isLeader = true
		index = rf.currentIndex
		rf.currentIndex++
		term = rf.currentTerm
		entry := RaftEntry{
			Index:index,
			Term: term,
			Command:command,
		}
		DPrintf("[Receive COMM] :%v receive :%v\n", rf.me, command)
		rf.raftLogs = append(rf.raftLogs, entry)
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}


//handle the comming msg from other raft instance
func (rf *Raft) DoHandle(msg ApplyMsg) {

}


// change raft 's role between followers,candidate,leader
func (rf *Raft) ChangeRaftRole(role int) {
	rf.voteFor = -1
	rf.votes = 0
	//DPrintf("[CHANGE ROLE]%v change role to :%v\n", rf.me, role)
	switch (role) {
	case RaftRoleCandidates :
		rf.mu.Lock()
		rf.role = RaftRoleCandidates
		rf.voteFor = rf.me
		rf.voteCh <- rf.me
		rf.currentTerm ++
		rf.mu.Unlock()
		peersLen := len(rf.peers)
		ra := RequestVoteArgs{
			Candidate:rf.me,
			Term:rf.currentTerm,
			LastLogIndex:rf.getLogLastIndex(),
			LastLogTerm:rf.raftLogs[len(rf.raftLogs) - 1].Term,
		}

		for i := 0; i < peersLen; i++ {
			go func(index int) {
				if index != rf.me {
					rp := &RequestVoteReply{}
					//DPrintf("[SendRequestVote] %v send request to :%v\n", rf.me, index)
					rf.sendRequestVote(index, ra, rp)
				}
			}(i)
		}
	case RaftRoleLeaders:
		rf.mu.Lock()
		rf.role = RaftRoleLeaders
		rf.currentIndex = rf.getLogLastIndex() + 1
		nextIndex := rf.raftLogs[len(rf.raftLogs) - 1].Index + 1
		rf.nextIndexs = make([]int, len(rf.peers))
		rf.matchIndexs = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndexs[i] = nextIndex
			rf.matchIndexs[i] = 0
		}
		rf.voteFor = -1
		rf.mu.Unlock()
		heartbeat := true
		rf.broadcastAppendEnties(heartbeat)

	case RaftRoleFollowers:
		rf.mu.Lock()
		rf.role = RaftRoleFollowers
		rf.voteFor = -1
		rf.mu.Unlock()
	}

}

//leader send hearbeat
func (rf *Raft) broadcastAppendEnties(isSendHearbeat bool) {

	//1.commit log
	//2.broadcastAppendEnties

	//commitlog
	// If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N (§5.3, §5.4).
	if rf.role == RaftRoleLeaders {
		//DPrintf("[SendAppendEntries] %v begin to send appendEnties,role:%v\n", rf.me, rf.role)
		rf.mu.Lock()

		baseIndex := rf.raftLogs[0].Index
		N := rf.lastCommitIndex
		lastIndex := rf.getLogLastIndex()

		for i := rf.lastCommitIndex + 1; i <= lastIndex; i++ {
			num := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndexs[j] >= i && rf.raftLogs[i - baseIndex].Term == rf.currentTerm {
					num++
				}
			}
			if 2 * num > (len(rf.peers) - 1) {
				N = i
			}
		}
		if N > rf.lastCommitIndex {
			rf.lastCommitIndex = N
			//todo commit apply to state machine
			rf.commitCh <- true

		}
		rf.mu.Unlock()
		//DPrintf("[SendAppendEntries] %v begin to send appendEnties,rf.role:%v 222222\n", rf.me,rf.role)

		peersLen := len(rf.peers)
		for i := 0; i < peersLen; i++ {
			if i != rf.me {
				preIndex := rf.nextIndexs[i] - 1
				//fmt.Printf(":%v preindex\n",preIndex)
				preLogTerm := rf.raftLogs[preIndex].Term
				var entries []RaftEntry

				if !isSendHearbeat {
					entries = rf.raftLogs[preIndex + 1 - baseIndex:]
				}
				appEnt := AppendEntries{
					PrevLogTerm:preLogTerm,
					PrevLogIndex:preIndex,
					Term:rf.currentTerm,
					LeaderID:rf.me,
					LeaderCommitIndex:rf.lastCommitIndex,
					Entries:entries,
				}
				appRep := &AppendEntriesReply{}
				DPrintf("[SendAppendEntries] :%v send to :%v,%v\n", rf.me, i, appEnt)
				go rf.sendAppendEntries(i, appEnt, appRep)
			}
		}
	}
}

func (rf *Raft) Loop() {
	for {
		switch (rf.role) {
		case RaftRoleFollowers:
			rf.runFollowers()
		case RaftRoleLeaders:
			rf.runLeaders()
		case RaftRoleCandidates:
			rf.runCandidates()
		}
	}
}

func (rf *Raft) runFollowers() {
	select {
	case <-rf.heartbeat:
	case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
		DPrintf("[timeout] %v 's role:%v\n", rf.me, rf.role)
		rf.ChangeRaftRole(RaftRoleCandidates)
	case <-rf.voteCh:
		DPrintf("[Get Vote]%v  Recevice vote on role followers\n", rf.me)
	}
}

func (rf *Raft) runLeaders() {
	rf.broadcastAppendEnties(false)
	time.Sleep(time.Millisecond * time.Duration(50))
}

func (rf *Raft) runCandidates() {
	select {
	case <-rf.heartbeat:
		rf.ChangeRaftRole(RaftRoleFollowers)
	case <-time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
		DPrintf("[Candidate timeout] %v 's role:%v\n", rf.me, rf.role)
		rf.ChangeRaftRole(RaftRoleCandidates)
	case i := <-rf.voteCh:
		rf.votes ++
		DPrintf("[VoteCh]%v receive vote from :%v,votes:%v\n", rf.me, i, rf.votes)
		if rf.votes > len(rf.peers) / 2 {
			DPrintf("change raft role:%v,%v\n", rf.me, RaftRoleLeaders)
			rf.ChangeRaftRole(RaftRoleLeaders)
		}
	}
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here.
	runtime.GOMAXPROCS(runtime.NumCPU())

	rf.voteCh = make(chan int, len(peers))
	rf.commitCh = make(chan bool, 3)
	rf.votes = -1
	rf.role = RaftRoleFollowers
	rf.raftLogs = append(rf.raftLogs, RaftEntry{Term:0})
	rf.currentTerm = 0
	rf.currentIndex = 0

	rf.config.hearbeatTimeout = time.Duration(rand.Int63() % 333 + 550) * time.Millisecond
	rf.config.leaderHearbeatTimeOut = time.Millisecond * 10
	DPrintf("make with config:%v,peers:%v\n", rf.config,peers)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.timeoutTimer = time.NewTimer(rf.config.hearbeatTimeout)
	rf.applyCh = applyCh
	go rf.Loop()

	go func() {
		for {
			select {
			case <-rf.commitCh:
				rf.mu.Lock()
				baseIndex := rf.raftLogs[0].Index
				for i := rf.lastApplied + 1; i <= rf.lastCommitIndex; i++ {
					msg := ApplyMsg{Index:i, Command:rf.raftLogs[i - baseIndex].Command}
					rf.applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []RaftEntry) []RaftEntry {

	var newLogEntries []RaftEntry
	newLogEntries = append(newLogEntries, RaftEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for index := len(log) - 1; index >= 0; index-- {
		if log[index].Index == lastIncludedIndex && log[index].Term == lastIncludedTerm {
			newLogEntries = append(newLogEntries, log[index + 1:]...)
			break
		}
	}

	return newLogEntries
}