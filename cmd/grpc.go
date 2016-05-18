package cmd

import (
	"fmt"

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

func (gr *grpcServer) StoreItem(ctx context.Context, sir *StoreItemRequest) (*StoreItemResponse, error) {
	return &StoreItemResponse{}, nil
}
