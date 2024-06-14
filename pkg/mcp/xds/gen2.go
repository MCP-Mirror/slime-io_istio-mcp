package xds

import (
	"encoding/json"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc/codes"

	"istio.io/libistio/pkg/env"
	istioversion "istio.io/libistio/pkg/version"
)

var controlPlane *corev3.ControlPlane

func init() {
	// The Pod Name (instance identity) is in PilotArgs, but not reachable globally nor from DiscoveryServer
	podName := env.RegisterStringVar("POD_NAME", "", "").Get()
	byVersion, err := json.Marshal(IstioControlPlaneInstance{
		Component: "slime-mcp-server",
		ID:        podName,
		Info:      istioversion.Info,
	})
	if err != nil {
		xdsLog.Warnf("XDS: Could not serialize control plane id: %v", err)
	}
	controlPlane = &corev3.ControlPlane{Identifier: string(byVersion)}
}

// ControlPlane identifies the instance and Istio version.
func ControlPlane() *corev3.ControlPlane {
	return controlPlane
}

// IstioControlPlaneInstance defines the format Istio uses for when creating Envoy config.core.v3.ControlPlane.identifier
type IstioControlPlaneInstance struct {
	// The Istio component type (e.g. "istiod")
	Component string
	// The ID of the component instance
	ID string
	// The Istio version
	Info istioversion.BuildInfo
}

// handleReqAck checks if the message is an ack/nack and handles it, returning true.
// If false, the request should be processed by calling the generator.
func (s *Server) handleReqAck(con *Connection, discReq *discovery.DiscoveryRequest) (*WatchedResource, bool) {
	// All NACKs should have ErrorDetail set !
	// Relying on versionCode != sentVersionCode as nack is less reliable.

	isAck := true

	t := discReq.TypeUrl
	con.mu.Lock()
	w := con.node.Active[t]
	if w == nil {
		w = NewWatchedResource(t)
		con.node.Active[t] = w
		isAck = false // newly watched resource
	}
	con.mu.Unlock()

	if discReq.ErrorDetail != nil {
		errCode := codes.Code(discReq.ErrorDetail.Code)
		xdsLog.Warnf("ADS: ACK ERROR %s %s:%s", con.ConID, errCode.String(), discReq.ErrorDetail.GetMessage())
		if s.InternalGen != nil {
			s.InternalGen.OnNack(con.node, discReq)
		}
		return w, true
	}

	if discReq.ResponseNonce == "" {
		isAck = false // initial request
	} else {
		// This is an ACK response to a previous message - but it may refer to a response on a previous connection to
		// a different XDS server instance.

		// GRPC doesn't send version info in NACKs for RDS. Technically if nonce matches
		// previous response, it is an ACK/NACK.
		if w.IsRecorded(discReq.ResponseNonce) {
			xdsLog.Debugf("ADS: ACK %s %s %s %v", con.ConID, discReq.VersionInfo, discReq.ResponseNonce,
				time.Since(w.LastSent))
			w.NonceAcked = discReq.ResponseNonce
		} else {
			xdsLog.Infof("ADS: Expired nonce received %s %s, sent %s %v, received %s",
				con.ConID, discReq.TypeUrl, w.NonceSent, w.NonceSentRecords, discReq.ResponseNonce)
			// This is an ACK for a resource sent on an older stream, or out of sync.
			// Send a response back.
			isAck = false
		}
	}

	// Change in the set of watched resource - regardless of ack, send new data.
	if !listEqualUnordered(w.ResourceNames, discReq.ResourceNames) {
		xdsLog.Debugf("ADS: received %s %s, watch %s, received %s",
			con.ConID, discReq.TypeUrl, w.ResourceNames, discReq.ResourceNames)
		isAck = false
		w.ResourceNames = discReq.ResourceNames
	}
	w.LastRequest = discReq

	return w, isAck
}

// handleCustomGenerator uses model.Generator to generate the response.
func (s *Server) handleCustomGenerator(con *Connection, req *discovery.DiscoveryRequest) error {
	w, isAck := s.handleReqAck(con, req)
	if isAck {
		return nil
	}

	// XdsResourceGenerator is the default generator for this connection. We want to allow
	// some types to use custom generators - for example EDS.
	g := s.findGenerator(w.TypeUrl, con.node)
	if g == nil {
		return nil
	}

	push := s.globalPushContext()
	return s.pushXds(g, con, push, w, nil)
}

// Called for config updates.
// Will not be called if ProxyNeedsPush returns false - ie. if the update
func (s *Server) pushGeneratorV2(con *Connection, push *PushContext, w *WatchedResource, updates XdsUpdates) error {
	return s.pushXds(con.node.XdsResourceGenerator, con, push, w, updates)
}

func (s *Server) pushXds(gen XdsResourceGenerator, con *Connection, push *PushContext, w *WatchedResource, updates XdsUpdates) error {
	genRes := gen.Generate(con.node, push, w, nil)
	if len(genRes.Data) == 0 {
		return nil // No push needed.
	}

	resp := &discovery.DiscoveryResponse{
		ControlPlane: ControlPlane(),
		TypeUrl:      w.TypeUrl,
		VersionInfo:  genRes.Version,
	}
	if s.o.IncPush {
		resp.Nonce = genRes.Version
	} else {
		resp.Nonce = nonce(genRes.Version)
	}

	if resp.Nonce == w.NonceSent {
		xdsLog.Debugf("XDS: skip PUSH %s for node:%s resources:%d as nonce: %s equals nonce sent", w.TypeUrl, con.node.ID, len(genRes.Data), resp.Nonce)
		return nil
	}

	err := con.send(resp)
	if err != nil {
		return err
	}
	w.LastSent = time.Now()

	xdsLog.Infof("XDS: PUSH %s for node:%s resources:%d size %d nonce: %s", w.TypeUrl, con.node.ID, len(genRes.Data), resp.Nonce)
	return nil
}

// initGenerators initializes generators to be used by XdsServer.
func (s *Server) initGenerators() {
	s.Generators["api"] = &APIGenerator{incPush: s.o.IncPush}
	s.InternalGen = &InternalGen{
		Server: s,
	}
	s.Generators["api/"+TypeURLConnections] = s.InternalGen
	s.Generators["event"] = s.InternalGen
}
