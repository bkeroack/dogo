package cmd

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
)

type grpcServer struct {
	n *Node
}

func (gr *grpcServer) FetchItems(ctx context.Context, req *FetchItemsRequest) (*FetchItemsResponse, error) {
	resp := FetchItemsResponse{}
	if !gr.n.store.IsLeader() {
		resp.RpcError.IsError = true
		resp.RpcError.ErrorType = RPCError_NOT_LEADER
		resp.RpcError.ErrorMessage = gr.n.store.Leader()
		return &resp, nil
	}
	var clvl ConsistencyLevel
	switch req.ConsistencyLevel {
	case FetchItemsRequest_NONE:
		clvl = None
	case FetchItemsRequest_STRONG:
		clvl = Strong
	default:
		resp.RpcError.IsError = true
		resp.RpcError.ErrorType = RPCError_BAD_REQUEST
		resp.RpcError.ErrorMessage = "unknown consistency level"
		return &resp, nil
	}
	items, err := gr.n.FetchItems(req.Names, clvl)
	if err != nil {
		resp.RpcError.IsError = true
		ie, ok := err.(InternalError)
		if ok && ie.InternalError() {
			resp.RpcError.ErrorType = RPCError_INTERNAL_ERROR
			resp.RpcError.ErrorMessage = fmt.Sprintf("internal error: %v", err)
			return &resp, nil
		}
		bq, ok := err.(BadQuery)
		if ok && bq.BadQuery() {
			resp.RpcError.ErrorType = RPCError_BAD_REQUEST
			resp.RpcError.ErrorMessage = fmt.Sprintf("bad query: %v", err)
			return &resp, nil
		}
		resp.RpcError.ErrorType = RPCError_UNKNOWN
		resp.RpcError.ErrorMessage = fmt.Sprintf("unknown error: %v", err)
		return &resp, nil
	}
	for _, item := range items {
		ri := &RPCItem{
			Name:       item.Name,
			Flags:      item.Flags,
			Expiration: item.Expiration.Unix(),
			Value:      item.Value,
		}
		resp.Items = append(resp.Items, ri)
	}
	return &resp, nil
}

func (gr *grpcServer) StoreItem(ctx context.Context, req *StoreItemRequest) (*StoreItemResponse, error) {
	resp := StoreItemResponse{}
	if !gr.n.store.IsLeader() {
		resp.RpcError.IsError = true
		resp.RpcError.ErrorType = RPCError_NOT_LEADER
		resp.RpcError.ErrorMessage = gr.n.store.Leader()
		return &resp, nil
	}
	var sp StorePolicy
	switch req.StorePolicy {
	case StoreItemRequest_INSERT_IF_NOT_EXISTS:
		sp = InsertIfNotExists
	case StoreItemRequest_INSERT_OR_UPDATE:
		sp = InsertOrUpdate
	case StoreItemRequest_UPDATE_IF_EXISTS:
		sp = UpdateIfExists
	default:
		resp.RpcError.IsError = true
		resp.RpcError.ErrorType = RPCError_BAD_REQUEST
		resp.RpcError.ErrorMessage = "unknown store policy"
		return &resp, nil
	}
	item := &Item{
		Name:       req.Item.Name,
		Value:      req.Item.Value,
		Size:       uint64(len(req.Item.Value)),
		Flags:      req.Item.Flags,
		Expiration: time.Unix(req.Item.Expiration, 0),
		Created:    time.Unix(req.Item.Created, req.Item.CreatedNano),
		LastUsed:   time.Unix(req.Item.LastUsed, req.Item.LastUsedNano),
	}
	err := gr.n.StoreItem(item, sp)
	if err != nil {
		resp.RpcError.IsError = true
		resp.RpcError.ErrorType = RPCError_INTERNAL_ERROR
		resp.RpcError.ErrorMessage = fmt.Sprintf("StoreItem error: %v", err)
		return &resp, nil
	}
	resp.RpcError.ErrorType = RPCError_NO_ERROR
	return &resp, nil
}

func handleRPCError(caller string, rpcerr *RPCError) error {
	switch rpcerr.ErrorType {
	case RPCError_INTERNAL_ERROR:
		return newInternalErrorError(fmt.Errorf("%v: rpc internal error: %v", caller, rpcerr.ErrorMessage))
	case RPCError_BAD_REQUEST:
		return newBadQueryError(fmt.Errorf("%v: query error: %v", caller, rpcerr.ErrorMessage))
	case RPCError_NOT_LEADER:
		return newInternalErrorError(fmt.Errorf("%v: remote is not leader and claims leader is %v", caller, rpcerr.ErrorMessage))
	case RPCError_NO_ERROR:
		return newInternalErrorError(fmt.Errorf("%v: no actual RPC error (this is a bug)", caller))
	case RPCError_UNKNOWN:
		return newInternalErrorError(fmt.Errorf("%v: rpc unknown error: %v", caller, rpcerr.ErrorMessage))
	default:
		return newInternalErrorError(fmt.Errorf("%v: rpc bad error type: %v: %v", caller, rpcerr.ErrorType, rpcerr.ErrorMessage))
	}
}

// ProxyFetch determines cluster leader and performs a remote fetch from it
func (n *Node) ProxyFetch(keys []string) ([]*Item, error) {
	items := []*Item{}
	c, err := n.getRPCClient()
	if err != nil {
		return items, newInternalErrorError(fmt.Errorf("ProxyFetch: error getting RPC connection: %v", err))
	}
	req := &FetchItemsRequest{
		Names:            keys,
		ConsistencyLevel: FetchItemsRequest_STRONG,
	}
	r, err := c.FetchItems(context.TODO(), req, nil)
	if err != nil {
		return items, newInternalErrorError(fmt.Errorf("ProxyFetch: error executing RPC: %v", err))
	}
	if r.RpcError.IsError {
		return items, handleRPCError("ProxyFetch", r.RpcError)
	}
	for _, ritem := range r.Items {
		item := &Item{
			Name:       ritem.Name,
			Value:      ritem.Value,
			Size:       uint64(len(ritem.Value)),
			Flags:      ritem.Flags,
			Expiration: time.Unix(ritem.Expiration, 0),
			Created:    time.Unix(ritem.Created, ritem.CreatedNano),
			LastUsed:   time.Unix(ritem.LastUsed, ritem.LastUsedNano),
		}
		items = append(items, item)
	}
	return items, nil
}

// ProxyStore determines cluster leader and performs a remote store
func (n *Node) ProxyStore(item *Item, policy StorePolicy) error {
	c, err := n.getRPCClient()
	if err != nil {
		return newInternalErrorError(fmt.Errorf("ProxyStore: error getting RPC connection: %v", err))
	}
	var sp StoreItemRequest_StorePolicy
	switch policy {
	case InsertIfNotExists:
		sp = StoreItemRequest_INSERT_IF_NOT_EXISTS
	case InsertOrUpdate:
		sp = StoreItemRequest_INSERT_OR_UPDATE
	case UpdateIfExists:
		sp = StoreItemRequest_UPDATE_IF_EXISTS
	default:
		return newInternalErrorError(fmt.Errorf("ProxyStore: unknown store policy: %v", policy))
	}
	cr, crn := splitTimestamp(item.Created)
	lu, lun := splitTimestamp(item.LastUsed)
	ritem := RPCItem{
		Name:         item.Name,
		Value:        item.Value,
		Flags:        item.Flags,
		Expiration:   item.Expiration.Unix(),
		Created:      cr,
		CreatedNano:  crn,
		LastUsed:     lu,
		LastUsedNano: lun,
	}
	req := &StoreItemRequest{
		Item:        &ritem,
		StorePolicy: sp,
	}
	r, err := c.StoreItem(context.TODO(), req, nil)
	if err != nil {
		return newInternalErrorError(fmt.Errorf("ProxyStore: error executing RPC: %v", err))
	}
	if r.RpcError.IsError {
		return handleRPCError("ProxyStore", r.RpcError)
	}
	return nil
}

// ProxyExpire determines cluster leader and performs a remote expiration
func (n *Node) ProxyExpire(key string) error {
	return nil
}
