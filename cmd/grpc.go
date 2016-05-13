package cmd

import "golang.org/x/net/context"

type grpcServer struct {
	n *Node
}

func (gr *grpcServer) FetchItems(ctx context.Context, fir *FetchItemsRequest) (*FetchItemsResponse, error) {
	return &FetchItemsResponse{}, nil
}

func (gr *grpcServer) StoreItem(ctx context.Context, sir *StoreItemRequest) (*StoreItemResponse, error) {
	return &StoreItemResponse{}, nil
}
