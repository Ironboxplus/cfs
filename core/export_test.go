package core

import "context"

// ReadCheckLease and CheckLease expose the commit path's lease checks to the
// tests of package core_test.
func (v *Volume) ReadCheckLease(ctx context.Context) error { return v.readCheckLease(ctx) }
func (v *Volume) CheckLease(ctx context.Context) error     { return v.checkLease(ctx) }
