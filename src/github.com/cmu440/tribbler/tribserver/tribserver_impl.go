package tribserver

import (
	"errors"
	"time"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/rpc"
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

	err1 := rpc.RegisterName("TribServer", tribrpc.Wrap(&tribServ))
	if err1 != nil {
                fmt.Println(err1)
                return nil, err1
        }
        rpc.HandleHTTP()
        l, err2 := net.Listen("tcp", ":"+myHostPort)
        if err2 != nil {
                fmt.Println(err2)
                return nil, err2
        }
        go http.Serve(l, nil)




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

	//check if this user present in server
	_,err := ts.libStore.Get(util.FormatUserKey(thisUsrId))

	if(err != nil) {
		//user not found
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	//check if subscribe user is present
	_,err = ts.libStore.Get(util.FormatUserKey(subscrId))

	if(err != nil) {
		reply.Status = tribrpc.NoSuchTargetUser
		return err
	}

	//add to list of subscribers
	err = ts.libStore.AppendToList(util.FormatSubListKey(thisUsrId),subscrId)

	if(err != nil) {
		return err
	}

	/* no error */
	reply.Status = tribrpc.OK
	return nil
}

/*
check if both values exist, then delete the sub
*/
func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	var thisUsrId string = args.UserID
	var subscrId string = args.TargetUserID

	//check if this user present in server
	_,err := ts.libStore.Get(util.FormatUserKey(thisUsrId))

	if(err != nil) {
		//user not found
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	//check if subscribe user is present
	_,err = ts.libStore.Get(util.FormatUserKey(subscrId))

	if(err != nil) {
		reply.Status = tribrpc.NoSuchTargetUser
		return err
	}

	//retrieve list of subsriber user IDs
	err = ts.libStore.RemoveFromList(util.FormatSubListKey(thisUsrId),subscrId)

	reply.Status = tribrpc.OK
	return err
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	/*
	formatsublistkey
	*/
	var thisUsrId string = args.UserID

	//check if user present in server
	_,err := ts.libStore.Get(util.FormatUserKey(thisUsrId))

	if(err != nil) {
		//user not found
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	subscrList,err := ts.libStore.GetList(util.FormatSubListKey(thisUsrId))

	if(err != nil) {
		return err
	}

	reply.Status = tribrpc.OK
	reply.UserIDs = subscrList

	reply.Status = tribrpc.OK
	return nil
}

/*
	-Check if user is present
	-Create the tribble
	-timestamp: go time
	-create a tribble
	-marshal it

	-Appendto list that post key to the usrID
	-Add to the map from post key -> marshalled tribble
	*/
func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {

	var thisUsrId string = args.UserID
	var content string = args.Contents

	//check if user present in server
	_,err := ts.libStore.Get(util.FormatUserKey(thisUsrId))

	if(err != nil) {
		//user not found
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	var timeNow time.Time = time.Now()

	newTribble := tribrpc.Tribble{
		UserID: thisUsrId,
		Posted: timeNow,
		Contents: content,
	}

	marshalTrib, err := json.Marshal(&newTribble)

	if(err != nil) {
		fmt.Println("Error marshalling")
		return err
	}

	var postKey string = util.FormatPostKey(thisUsrId,timeNow.Unix())

	//store the tribble itself
	err = ts.libStore.Put(postKey, string(marshalTrib))

	if(err != nil) {
		fmt.Println("Error putting tribble contents")
		return err
	}

	//store the postkey
	err = ts.libStore.AppendToList(util.FormatTribListKey(thisUsrId),postKey)

	if(err != nil) {
		fmt.Println("Error putting postKey")
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

/*
	2 deletes:
	-remove the tribble itself
	-remove the postkey
	*/

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {

	var usrID string = args.UserID
	var postKey string = args.PostKey

	//check if user present in server
	_,err := ts.libStore.Get(util.FormatUserKey(usrID))

	if(err != nil) {
		//user not found
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	//check if postKey is stored
	_,err = ts.libStore.Get(postKey)

	if(err != nil) {
		reply.Status = tribrpc.NoSuchPost
		return err
	}

	errDelPost := ts.libStore.Delete(postKey)
	errDelKey :=
		ts.libStore.RemoveFromList(util.FormatTribListKey(usrID), postKey)

	if(errDelPost != nil) {
		fmt.Println("Error deleting post")
		return errDelPost
	}

	if(errDelKey != nil) {
		fmt.Println("Error removing key from list")
		return errDelKey
	}

	reply.Status = tribrpc.OK
	return nil
}

/*
-getList, formatTribListKey to get all the post keys
-Slice it off at 100
-reverse it or wtv (if needed)
-Get() with that post key, and get the marshalled tribble
*/
func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {

	var usrID string = args.UserID

	//check if user present in server
	_,err := ts.libStore.Get(util.FormatUserKey(usrID))

	if(err != nil) {
		//user not found
		reply.Status = tribrpc.NoSuchUser
		return err
	}

	postKeysList,err := ts.libStore.GetList(util.FormatTribListKey(usrID))

	if(err != nil) {
		fmt.Println("Could not get list of postKeys for user")
		return err
	}

	//min(100, length of slice)
	var sliceSize int = int(math.Min(float64(len(postKeysList)),100))

	//create new list of tribbles
	tribList := make([]tribrpc.Tribble,sliceSize)

	for idx,currKey := range(postKeysList[:sliceSize]) {

		marshalStr,err := ts.libStore.Get(currKey)
		
		if(err != nil) {
			fmt.Println("Error retrieving currKey")
		
		} else {

			var currTrib tribrpc.Tribble
			err = json.Unmarshal([]byte(marshalStr), &currTrib)

			if(err != nil) {
				fmt.Println("Error Unmarshalling")

			} else {
				tribList[idx] = currTrib
			}
		}
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = tribList

	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	/*
	-format sublist
	*/
	return errors.New("not implemented")
}
