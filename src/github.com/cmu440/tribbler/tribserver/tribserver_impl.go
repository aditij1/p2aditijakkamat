package tribserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"time"
)

const TIMEOUT_GETTING_SERVERS = "GET_SERVER_TIMEOUT"
const WRONG_SERVER = "WRONG_SERVER"
const KEY_NOT_FOUND = "KEY_NOT_FOUND"
const ITEM_EXISTS = "ITEM_EXISTS"
const ITEM_NOT_FOUND = "ITEM_NOT_FOUND"
const KEY_NOT_CACHED = "REVOKE_FAILED_KEY_NOT_CACHED"

const ERROR_DIAL_TCP = "ERROR_DIAL_TCP"
const UNEXPECTED_ERROR = "UNEXPECTED_ERROR"

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
	libStore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	//fmt.Println("Creating new tribServer...")
	if err != nil {
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
	l, err2 := net.Listen("tcp", myHostPort)
	if err2 != nil {
		fmt.Println(err2)
		return nil, err2
	}
	go http.Serve(l, nil)

	return &tribServ, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	fmt.Println("-----> CreateUser")
	var usrID string = args.UserID

	_, err := ts.libStore.Get(util.FormatUserKey(usrID))

	if err == nil {
		//user not found
		reply.Status = tribrpc.Exists
		return nil
		//return err
	}

	err = ts.libStore.Put(util.FormatUserKey(usrID), "exists")

	if err != nil {

		return nil
	}

	//populate reply struct
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	fmt.Println("-----> AddSubscription")
	/*
		-Check if user present
		-Check if user being subscribed to is present
		-append to list: sublist key formatter,
	*/

	var thisUsrId string = args.UserID
	var subscrId string = args.TargetUserID

	//check if this user present in server
	_, err := ts.libStore.Get(util.FormatUserKey(thisUsrId))

	if err != nil {

		switch err.Error() {
		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchUser
			return nil

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
			return nil
		}
	}

	//check if subscribe user is present
	_, err = ts.libStore.Get(util.FormatUserKey(subscrId))

	if err != nil {
		switch err.Error() {
		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchTargetUser
			return nil

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
		}
	}

	//add to list of subscribers
	err = ts.libStore.AppendToList(util.FormatSubListKey(thisUsrId), subscrId)

	if err != nil {
		//TODO after checkpoint, case on err
		reply.Status = tribrpc.Exists
		return nil
	}

	/* no error */
	reply.Status = tribrpc.OK
	return nil
}

/*
check if both values exist, then delete the sub
*/
func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	fmt.Println("-----> RemoveSubscription")
	var thisUsrId string = args.UserID
	var subscrId string = args.TargetUserID

	//check if this user present in server
	_, err := ts.libStore.Get(util.FormatUserKey(thisUsrId))

	if err != nil {

		switch err.Error() {
		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchUser
			return nil

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
		}
	}

	//check if subscribe user is present
	_, err = ts.libStore.Get(util.FormatUserKey(subscrId))

	if err != nil {
		switch err.Error() {
		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchTargetUser
			return nil

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
			return nil
		}
	}

	//retrieve list of subsriber user IDs
	err = ts.libStore.RemoveFromList(util.FormatSubListKey(thisUsrId), subscrId)

	if err == nil {
		reply.Status = tribrpc.OK
		return nil
	}

	switch err.Error() {
	case ITEM_NOT_FOUND:
		//User present, no subscriptions
		reply.Status = tribrpc.NoSuchTargetUser
		//Empty list of user IDs
		return nil

	case WRONG_SERVER:
		fmt.Println("ERROR: WRONG SERVER in tribserver")
		return errors.New("Wrong server contacted!")

	case KEY_NOT_FOUND:
		fmt.Println("TribServ: Key was no found!")
		reply.Status = tribrpc.NoSuchTargetUser
		return nil

	default:
		fmt.Println("ERROR in tribserver: wrong error message received")
	}

	if err != nil {
		//TODO after checkpoint, case on err
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	fmt.Println("-----> GetSubscriptions")
	/*
		formatsublistkey
	*/
	var thisUsrId string = args.UserID

	//check if user present in server
	_, err := ts.libStore.Get(util.FormatUserKey(thisUsrId))

	if err != nil {
		switch err.Error() {
		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchUser
			return nil

		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
			return errors.New("WRONG error message")
		}

		//user not found
		reply.Status = tribrpc.NoSuchUser
		//return err
		return nil
	}

	subscrList, err := ts.libStore.GetList(util.FormatSubListKey(thisUsrId))

	if err == nil {
		reply.Status = tribrpc.OK
		reply.UserIDs = subscrList
		return nil
	}

	//error not nil

	switch err.Error() {
	case KEY_NOT_FOUND:
		//User present, no subscriptions
		reply.Status = tribrpc.OK
		//Empty list of user IDs
		reply.UserIDs = make([]string, 0)
		return nil
		break

	case WRONG_SERVER:
		fmt.Println("ERROR: WRONG SERVER in tribserver")
		return nil
		break
	default:
		fmt.Println("ERROR in tribserver: wrong error message received")
	}

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

	fmt.Println("-----> PostTribble")
	var thisUsrId string = args.UserID
	var content string = args.Contents

	//check if user present in server
	_, err := ts.libStore.Get(util.FormatUserKey(thisUsrId))

	if err != nil {
		switch err.Error() {
		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchUser
			return nil

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
		}
		//user not found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	var timeNow time.Time = time.Now()

	newTribble := tribrpc.Tribble{
		UserID:   thisUsrId,
		Posted:   timeNow,
		Contents: content,
	}

	marshalTrib, err := json.Marshal(&newTribble)

	if err != nil {
		fmt.Println("Error marshalling")
		//return err
	}

	var postKey string = util.FormatPostKey(thisUsrId, timeNow.UnixNano())

	//store the tribble itself
	err = ts.libStore.Put(postKey, string(marshalTrib))

	if err != nil {
		fmt.Println("Error putting tribble contents")
		//return err
	}

	//store the postkey
	err = ts.libStore.AppendToList(util.FormatTribListKey(thisUsrId), postKey)

	if err != nil {
		fmt.Println("Error putting postKey")
		//return err
	}

	reply.PostKey = postKey
	reply.Status = tribrpc.OK
	return nil
}

/*
	2 deletes:
	-remove the tribble itself
	-remove the postkey
*/

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	fmt.Println("-----> Delete(), postKey: ", args.PostKey)

	var usrID string = args.UserID
	var postKey string = args.PostKey

	fmt.Println("Delete(), postKey: ", postKey)
	//check if user present in server
	_, err := ts.libStore.Get(util.FormatUserKey(usrID))

	if err != nil {
		switch err.Error() {
		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchUser
			return nil

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
		}

	}

	//check if postKey is stored
	_, err = ts.libStore.Get(postKey)

	if err != nil {
		switch err.Error() {
		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchPost
			return nil

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
			return nil
		}
	}

	errDelPost := ts.libStore.Delete(postKey)
	errDelKey :=
		ts.libStore.RemoveFromList(util.FormatTribListKey(usrID), postKey)

	if errDelPost != nil {
		fmt.Println("Error deleting post")
		return nil
	}

	if errDelKey != nil {
		fmt.Println("Error removing key from list")
		return nil
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

	fmt.Println("-----> GetTribbles")
	var usrID string = args.UserID

	//check if user present in server
	_, err := ts.libStore.Get(util.FormatUserKey(usrID))

	if err != nil {
		switch err.Error() {
		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchUser
			return nil

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
		}
	}

	postKeysList, err := ts.libStore.GetList(util.FormatTribListKey(usrID))

	if err != nil {
		//fmt.Println("Could not get list of postKeys for user")
		//return empty tribble list, as the list is not yet created (0 tribbles)

		switch err.Error() {
		case KEY_NOT_FOUND:
			reply.Status = tribrpc.OK
			reply.Tribbles = make([]tribrpc.Tribble, 0)
			return nil
		case WRONG_SERVER:
			fmt.Println("WRONG SERVER CONTACTED!")
			return nil
		default:
			fmt.Println("ERROR in tribserver: wrong error message received!")
			return nil
		}

	}

	reply.Status = tribrpc.OK
	reply.Tribbles = ts.getTribbleList(postKeysList)

	return nil
}

func (ts *tribServer) getTribbleList(postKeysList []string) []tribrpc.Tribble {
	fmt.Println("-----> getTribbleList")
	//min(100, length of slice)
	var sliceSize int = int(math.Min(float64(len(postKeysList)), 100))

	//create new list of tribbles
	tribList := make([]tribrpc.Tribble, sliceSize)

	for i := 0; i < sliceSize; i++ {

		currKey := postKeysList[len(postKeysList)-i-1]

		marshalStr, err := ts.libStore.Get(currKey)

		if err != nil {
			fmt.Println("Error retrieving currKey")

		} else {

			var currTrib tribrpc.Tribble
			err = json.Unmarshal([]byte(marshalStr), &currTrib)

			if err != nil {
				fmt.Println("Error Unmarshalling")

			} else {
				tribList[i] = currTrib
			}
		}
	}

	return tribList
}

//defining type for sorting
type PostByTime []string

func (pkSlice PostByTime) Len() int {
	return len(pkSlice)
}

func (pkSlice PostByTime) Swap(i, j int) {
	pkSlice[i], pkSlice[j] = pkSlice[j], pkSlice[i]
}

func (pkSlice PostByTime) Less(i, j int) bool {
	var key1 []string = strings.Split(pkSlice[i], "_")
	var key2 []string = strings.Split(pkSlice[j], "_")

	//fmt.Println("Attempting to parse:",  key1[1]  +
	//	", " + key2[1])
	time1, err1 := strconv.ParseInt(key1[1], 16, 64)
	time2, err2 := strconv.ParseInt(key2[1], 16, 64)

	if err1 != nil || err2 != nil {
		fmt.Println("Error parsing int!")
	}

	//reverse chronological order?
	//fmt.Printf("%V < %V : %V\n", time1, time2, (time1 < time2))
	return time1 < time2
}

/*
-format sublist
*/
func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {

	fmt.Println("-----> GetTribblesBySubscription")
	var usrID string = args.UserID

	//check if user present in server
	_, err := ts.libStore.Get(util.FormatUserKey(usrID))

	if err != nil {
		switch err.Error() {
		case WRONG_SERVER:
			fmt.Println("ERROR: WRONG SERVER in tribserver")
			return errors.New("Wrong server contacted!")

		case KEY_NOT_FOUND:
			reply.Status = tribrpc.NoSuchUser
			return nil

		default:
			fmt.Println("ERROR in tribserver: wrong error message received")
		}
	}

	//get list of subscribers
	subscrList, err := ts.libStore.GetList(util.FormatSubListKey(usrID))

	if err != nil {
		//return err
		fmt.Println("No subscribers, or error getting list of subscribers")
		reply.Status = tribrpc.OK
		return nil
	}

	//initialise empty slice of all postKeys from subscribers
	var allPostKeys []string = make([]string, 0)

	//populate allPostKeys
	for _, currUser := range subscrList {
		currPostKeys, err := ts.libStore.GetList(util.FormatTribListKey(currUser))

		if err == nil {
			allPostKeys = append(allPostKeys, currPostKeys...)

		} else {
			//fmt.Println("0 tribs for user detected")
		}
	}

	sort.Sort(PostByTime(allPostKeys))

	//choose most recent posts, and get tribbles
	var tribList []tribrpc.Tribble = ts.getTribbleList(allPostKeys)

	reply.Tribbles = tribList
	reply.Status = tribrpc.OK
	return nil

}
