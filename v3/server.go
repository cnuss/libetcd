package v3

import (
	"go.etcd.io/etcd/server/v3/etcdserver"
)

type Server interface {
	etcdserver.ServerV3
}
