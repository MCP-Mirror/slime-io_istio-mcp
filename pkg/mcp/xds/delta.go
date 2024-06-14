package xds

import (
	"errors"
	"fmt"
	"strings"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"

	"istio.io/istio-mcp/pkg/mcp"
	"istio.io/istio-mcp/pkg/model"
	"istio.io/libistio/pkg/config"
	istiolog "istio.io/libistio/pkg/log"
	"istio.io/libistio/pkg/util/sets"
)

var deltaLog = istiolog.RegisterScope("mcp-delta-xds", "delta ads debugging")

// DeltaDiscoveryStream is a server interface for Delta XDS.
type DeltaDiscoveryStream = discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer

type deltaConnection struct {
	Connection
	// deltaStream is used for Delta XDS. Only one of deltaStream or stream will be set
	deltaStream DeltaDiscoveryStream

	deltaReqChan chan *discovery.DeltaDiscoveryRequest
}

func (conn *deltaConnection) send(res *discovery.DeltaDiscoveryResponse) error {
	return conn.Connection.sendResponse(res.TypeUrl, res.SystemVersionInfo, res.Nonce, func() error {
		return conn.deltaStream.Send(res)
	})
}

func newDeltaConnection(peerAddr string, stream DeltaDiscoveryStream) *deltaConnection {
	return &deltaConnection{
		Connection:   *newConnection(peerAddr, nil),
		deltaStream:  stream,
		deltaReqChan: make(chan *discovery.DeltaDiscoveryRequest, 1),
	}
}

func (s *Server) DeltaAggregatedResources(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) error {
	ctx := stream.Context()
	peerAddr := "0.0.0.0"
	if peerInfo, ok := peer.FromContext(ctx); ok {
		peerAddr = peerInfo.Addr.String()
	}
	con := newDeltaConnection(peerAddr, stream)
	var receiveError error
	go s.receiveDelta(con, &receiveError)
	for {
		select {
		case req, ok := <-con.deltaReqChan:
			if !ok {
				return receiveError
			}
			deltaLog.Infof("ADS: %s received delta request %v", con.ConID, req)
			err := s.processDeltaRequest(req, con)
			if err != nil {
				deltaLog.Errorf("ADS: %s failed to process delta request: %v", con.ConID, err)
				return err
			}
		case pushEv := <-con.pushChannel:
			pushEv = con.MergeAndClearPendingEv(pushEv)
			err := s.pushConnectionDelta(con, pushEv)
			if done := pushEv.done; done != nil {
				pushEv.done()
			}
			if err != nil {
				deltaLog.Errorf("pushConnectionDelta for %s met err %v", con.ConID, err)
				return err
			}
		}
	}
}

func (s *Server) receiveDelta(con *deltaConnection, errP *error) {
	firstReq := true
	for {
		req, err := con.deltaStream.Recv()
		if err != nil {
			if isExpectedGRPCError(err) {
				deltaLog.Infof("ADS: %s terminated %v", con.ConID, err)
				return
			}
			*errP = err
			deltaLog.Errorf("ADS: %s terminated with error: %v", con.ConID, err)
			return
		}
		if firstReq {
			firstReq = false
			if req.Node == nil || req.Node.Id == "" {
				*errP = errors.New("missing node ID")
				return
			}
			if err := s.initConnection(req.Node, &con.Connection); err != nil {
				*errP = err
				return
			}
			defer func() {
				s.removeCon(con.ConID)
				ev := mcp.ClientEvent{
					Clients:   []mcp.ClientInfo{con.clientInfo()},
					EventType: mcp.ClientEventDelete,
				}
				for _, h := range s.clientEventHandlers {
					h(ev)
				}
				if s.InternalGen != nil {
					s.InternalGen.OnDisconnect(&con.Connection)
				}
			}()
		}
		select {
		case con.deltaReqChan <- req:
		case <-con.deltaStream.Context().Done():
			deltaLog.Infof("ADS: %s terminated with stream closed", con.ConID)
			return
		}
	}

}

func (s *Server) processDeltaRequest(req *discovery.DeltaDiscoveryRequest, con *deltaConnection) error {
	w, should := s.shouleResponseDelta(req, con)
	if !should {
		return nil
	}

	g := s.findGenerator(req.TypeUrl, con.node)
	if g == nil {
		return nil
	}

	push := s.globalPushContext()
	gen, ok := g.(XdsDeltaResourceGenerator)
	if !ok {
		return errors.New("generator not found for delta request")
	}
	return s.pushDeltaXds(gen, con, push, w, nil)
}

// shouldResponseDelta returns true if the delta xds request should be process.
// 1. ErrorDetail != nil: nack, needless response
// 2. ErrorDetail == nil and RecordNonce: ack, needless response
// 3. pre watchedResources == nil: initial request or server restart, need response
// 4. pre watchedResources != nil: Spontaneous DeltaDiscoveryRequests, not support for now
func (s *Server) shouleResponseDelta(req *discovery.DeltaDiscoveryRequest, con *deltaConnection) (*WatchedResource, bool) {
	// nack needless response
	if req.ErrorDetail != nil {
		errCode := codes.Code(req.ErrorDetail.GetCode())
		deltaLog.Warnf("ADS:%s: received nack from %s for delta request %v, errCode:%v", req.TypeUrl, con.ConID, req, errCode)
		if s.InternalGen != nil {
			s.InternalGen.OnDeltaNack(con.node, req)
		}
		return nil, false
	}

	gvk, ok := typeUrlToGvk(req.TypeUrl)
	if !ok {
		return nil, false
	}

	con.mu.Lock()
	pre := con.node.Active[req.TypeUrl]
	con.mu.Unlock()

	if pre == nil {
		// initial request or reconnection
		if len(req.InitialResourceVersions) > 0 {
			// reconnection, server restart
			deltaLog.Debugf("ADS:%s: reconnection from %s with res: %v", req.TypeUrl, con.ConID, req.InitialResourceVersions)
		} else {
			// initial request
			deltaLog.Debugf("ADS:%s: initial request from %s", req.TypeUrl, con.ConID)
		}
		con.mu.Lock()
		pre = NewWatchedResource(req.TypeUrl)
		con.node.Active[req.TypeUrl] = pre
		res, wildcard := deltaWatchedResources(pre.ResourceNames, req)
		pre.ResourceNames = res
		pre.Wildcard = wildcard

		for _, n := range res {
			nsn := strings.Split(n, "/")
			key := model.ConfigKey{
				Kind:      gvk,
				Name:      nsn[1],
				Namespace: nsn[0],
			}
			pre.RecordResourceSent(key)
		}
		con.mu.Unlock()
		return pre, true
	}

	// ack needless response
	if req.ResponseNonce != "" && pre.IsRecorded(req.ResponseNonce) {
		if req.ResponseNonce == pre.NonceSent {
			// ack
			con.mu.Lock()
			pre.NonceAcked = req.ResponseNonce
			pre.VersionAcked = pre.VersionSent
			con.mu.Unlock()
			deltaLog.Debugf("ADS:%s: received ack of %s from %s ", req.TypeUrl, req.ResponseNonce, con.ConID)
		} else {
			deltaLog.Debugf("ADS:%s: received expired nonce %s from %s, last sent: %s", req.TypeUrl, req.ResponseNonce, con.ConID, pre.NonceSent)
		}
		return nil, false
	}

	// NOTE: mcp not support for Spontaneous DeltaDiscoveryRequests from the client.
	// This can be done to dynamically add or remove elements from the tracked resource_names set.
	// In this case response_nonce must be omitted.
	return nil, false

}

// deltaWatchedResources returns current watched resources of delta xds
func deltaWatchedResources(existing []string, request *discovery.DeltaDiscoveryRequest) ([]string, bool) {
	res := sets.New(existing...)
	res.InsertAll(request.ResourceNamesSubscribe...)
	// This is set by Envoy on first request on reconnection so that we are aware of what Envoy knows
	// and can continue the xDS session properly.
	for k := range request.InitialResourceVersions {
		res.Insert(k)
	}
	res.DeleteAll(request.ResourceNamesUnsubscribe...)
	wildcard := false
	// A request is wildcard if they explicitly subscribe to "*" or subscribe to nothing
	if res.Contains("*") {
		wildcard = true
		res.Delete("*")
	}
	// "if the client sends a request but has never explicitly subscribed to any resource names, the
	// server should treat that identically to how it would treat the client having explicitly
	// subscribed to *"
	// NOTE: this means you cannot subscribe to nothing, which is useful for on-demand loading; to workaround this
	// Istio clients will send and initial request both subscribing+unsubscribing to `*`.
	if len(request.ResourceNamesSubscribe) == 0 {
		wildcard = true
	}
	return res.UnsortedList(), wildcard
}

// Note: this is the style used by MCP and its config. Pilot is using 'Group/Version/Kind' as the
// key, which is similar.
//
// The actual type in the Any should be a real proto - which is based on the generated package name.
// For example: type is for Any is 'type.googlepis.com/istio.networking.v1alpha3.EnvoyFilter
// We use: networking.istio.io/v1alpha3/EnvoyFilter
func typeUrlToGvk(typeUrl string) (config.GroupVersionKind, bool) {
	parts := strings.Split(typeUrl, "/")
	if len(parts) != 3 {
		return config.GroupVersionKind{}, false
	}
	return config.GroupVersionKind{
		Group:   parts[0],
		Version: parts[1],
		Kind:    parts[2],
	}, true
}

func (s *Server) pushConnectionDelta(con *deltaConnection, ev *Event) error {
	if !ProxyNeedsPush(con.node, ev) {
		return nil
	}
	deltaLog.Infof("Pushing %v", con.ConID)

	if con.node.XdsResourceGenerator != nil {
		for _, w := range con.node.Active {
			err := s.pushDeltaGenerator(con, ev.push, w, ev.configsUpdated)
			if err != nil {
				return fmt.Errorf("pushDeltaGenerator typeurl %s mer err %v", w.TypeUrl, err)
			}
		}
	}
	return nil
}

func (s *Server) pushDeltaGenerator(con *deltaConnection, push *PushContext, w *WatchedResource, updates XdsUpdates) error {
	gen, ok := con.node.XdsResourceGenerator.(XdsDeltaResourceGenerator)
	if !ok {
		return errors.New("generator not found for delta request")
	}
	return s.pushDeltaXds(gen, con, push, w, updates)
}

func (s *Server) pushDeltaXds(gen XdsDeltaResourceGenerator, con *deltaConnection, push *PushContext, w *WatchedResource, updates XdsUpdates) error {
	res, removedRes := gen.GenerateDeltas(con.node, push, w, updates)
	resp := &discovery.DeltaDiscoveryResponse{
		ControlPlane:      ControlPlane(),
		TypeUrl:           w.TypeUrl,
		Nonce:             nonce(res.Version),
		SystemVersionInfo: res.Version,
		Resources:         res.Data,
		RemovedResources:  removedRes,
	}
	if resp.Nonce == w.NonceSent {
		deltaLog.Debugf("ADS:%s:  skip delta push for node:%s resources:%d as nonce: %s equals nonce sent", w.TypeUrl, con.node.ID, len(res.Data), resp.Nonce)
	}

	err := con.send(resp)
	if err != nil {
		return err
	}
	w.LastSent = time.Now()

	deltaLog.Infof("ADS:%s: pushed delta response to %s, count=%d remove=%d nonce=%s",
		w.TypeUrl, con.ConID, len(res.Data), len(removedRes), resp.Nonce)
	return nil
}
