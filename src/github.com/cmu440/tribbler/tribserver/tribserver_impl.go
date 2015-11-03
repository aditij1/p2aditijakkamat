package tribserver

import (
	"errors"
	//"net"
	//"net/rpc"
	"github.com/cmu440/tribbler/libstore"
	//"github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	//"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/util"
)

type tribServer struct {
	libStore libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	libStore,err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)

	if(err != nil) {
		return nil, err
	}

	var tribServ tribServer = tribServer{
		libStore: libStore,
	}


	return &tribServ, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {

	var usrID string = args.UserID

	err := ts.libStore.Put(util.FormatUserKey(usrID), "exists")

	if(err != nil) {

		return err
	}

	//populate reply struct
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	/*
	-Check if user present
	-Check if user being subscribed to is present
	-append to list: sublist key formatter,
	*/	

	var thisUsrId string = args.UserID
	var subscrId string = args.TargetUserID

	_,err := ts.libStore.Get(util.FormatUserKey(thisUsrId))

	if(err != nil) {
		//user not found
		return err
	}
	//add to list of subscribers
	err = ts.libStore.AppendToList(util.FormatUserKey(thisUsrId),subscrId)

	if(err != nil) {
		return err
	}

	/* no error */
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	
	/*
	check if both values exist, then delete the sub
	*/
	return errors.New("not implemented")
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	/*
	formatsublistkey
	
	*/
	return errors.New("not implemented")
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	/*
	-Check if user is present
	-Create the tribble
	-timestamp: go time
	-create a tribble
	-marshal it

	-Appendto list that post key to the usrID
	-Add to the map from post key -> marshalled tribble

	*/

	return errors.New("not implemented")
}


func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	/*
	2 deletes:
	-remove the tribble itself
	-remove the postkey
	*/
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	/*
	-getList, formatTribListKey to get all the post keys 
	-Slice it off at 100 
	-reverse it or wtv (if needed)
	-Get() with that post key, and get the marshalled tribble
	*/
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	/*
	-format sublist
	*/
	return errors.New("not implemented")
}
