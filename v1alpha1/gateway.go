package v1alpha1

import (
	"context"
	"net/http"
	"strings"

	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	etcdservergw "go.etcd.io/etcd/api/v3/etcdserverpb/gw"
	v3electiongw "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb/gw"
	v3lockgw "go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb/gw"
)

// grpcHandlerFunc multiplexes gRPC and a fallback HTTP handler onto one
// http.Handler (for an HTTP/2 listener), mirroring embed's grpcHandlerFunc.
// With a nil fallback it serves gRPC only.
func grpcHandlerFunc(gs *grpc.Server, other http.Handler) http.Handler {
	if other == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gs.ServeHTTP(w, r)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			gs.ServeHTTP(w, r)
		} else {
			other.ServeHTTP(w, r)
		}
	})
}

// h2cHandler wraps h so it serves HTTP/2 over cleartext (h2c), letting gRPC and
// the gateway's loopback dial speak HTTP/2 on a plaintext listener. Non-h2c
// requests fall through to h.
func h2cHandler(h http.Handler) http.Handler {
	return h2c.NewHandler(h, &http2.Server{})
}

// gatewayMux builds the grpc-gateway REST mux for the etcd v3 API, backed by a
// lazy gRPC connection to target (the client listener's address). It mirrors
// embed's unexported registerGateway using the exported gw register funcs. The
// connection dials on first request, so target need not be serving yet; it is
// not closed here and lives with the returned mux.
func gatewayMux(target string) (*gwruntime.ServeMux, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	mux := gwruntime.NewServeMux(
		gwruntime.WithMarshalerOption(gwruntime.MIMEWildcard,
			&gwruntime.HTTPBodyMarshaler{
				Marshaler: &gwruntime.JSONPb{
					MarshalOptions:   protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false},
					UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
				},
			},
		),
	)
	ctx := context.Background()
	register := []func(context.Context, *gwruntime.ServeMux, *grpc.ClientConn) error{
		etcdservergw.RegisterKVHandler,
		etcdservergw.RegisterWatchHandler,
		etcdservergw.RegisterLeaseHandler,
		etcdservergw.RegisterClusterHandler,
		etcdservergw.RegisterMaintenanceHandler,
		etcdservergw.RegisterAuthHandler,
		v3lockgw.RegisterLockHandler,
		v3electiongw.RegisterElectionHandler,
	}
	for _, reg := range register {
		if err := reg(ctx, mux, conn); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return mux, nil
}
