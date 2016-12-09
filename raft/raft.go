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
	RaftRoleFollowers = 0 << iota
	RaftRoleCandidates
	RaftRoleLeaders
)

type raftConfig struct {
	hearbeadTimeout time.Time
}
type Raft struct {
	mu            sync.Mutex
	peers         []*labrpc.ClientEnd
	persister     *Persister
	me            int          // index into peers[]

                               // Your data here.
                               // Look at the paper's Figure 2 for a description of what
                               // state a Raft server must maintain.
	config       raftConfig // some config
	currentTerm   int          //currentTermlatest term server has seen (initialized to 0on first boot, increases monotonically
	currentIndex  int

	lastCommitIndex int
	nextIndexs   []int  //The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower.

	raftLogs      []*raftEntry //log entries;each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	role          int

	hearbeatTimer time.Timer
	applyCh       chan ApplyMsg
	candidateCh   chan time.Time

	Votes  int

}

type raftEntry struct {
	index int
	command string
	term    int
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
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Candidate int
	Term      int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	votefor int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

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
	return ok
}



type AppendEntries struct {
	Term              int
	LeaderID          int
	LeaderCommitIndex int
}

type AppendEntriesReply struct{

}
//
func (rf *Raft) AppendEntriesRPC(args AppendEntries,reply *AppendEntriesReply){
	//reset the hearbeat time out

	rf.hearbeatTimer.Reset(rf.config.hearbeadTimeout)

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
	isLeader := true

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
	switch role {
	case RaftRoleCandidates :
		rf.role = RaftRoleCandidates
		rf.currentTerm ++
		peersLen := len(rf.peers)
		ra := RequestVoteArgs{Candidate:rf.me,Term:rf.currentTerm}
		rp := &RequestVoteReply{}
		for i := 0 ;i< peersLen;i++{
			if rf.sendRequestVote(i,ra,rp) && rp.votefor == rf.me{
				rf.Votes ++
				if rf.Votes > peersLen/2{
					rf.ChangeRaftRole(RaftRoleLeaders)
				}
			}
		}
	case RaftRoleLeaders:

	}

}

//leader send hearbeat
func (rf *Raft) sendHearbeat() {
	if rf.role == RaftRoleLeaders {

		//for  := range rf.peers {
		//
		//}
	}
}

func (rf *Raft) Loop() {
	//
	select {
	case <-rf.hearbeatTimer.C:
		switch (rf.role){
		case RaftRoleFollowers:
			rf.ChangeRaftRole(RaftRoleCandidates)
		case RaftRoleLeaders:
			rf.sendHearbeat()
		case RaftRoleCandidates:

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
	rf.Votes = 0

	// Your initialization code here.
	rf.config.hearbeadTimeout = time.Second
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.hearbeatTimer = time.NewTimer(rf.config.hearbeadTimeout)
	rf.applyCh = applyCh
	go rf.Loop()

	return rf
}
