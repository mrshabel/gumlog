// this file contains code enforces an Access Control List (ACL) rules/policy on connected clients
package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

// the New function returns an authorization enforcer instance where model points to the file
// containing the casbin's authorization setup and policy points to the csv file containing the
// ACL table
func New(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

// this function checks whether a given subject can access and perform an action on a given object/resource
func (a *Authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		errMsg := fmt.Sprintf("%s not permitted to %s to %s", subject, action, object)
		st := status.New(codes.PermissionDenied, errMsg)
		return st.Err()
	}
	return nil
}
