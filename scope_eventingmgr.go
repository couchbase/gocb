package gocb

// ScopeEventingFunctionManager provides methods for performing scoped eventing function management operations.
// This manager is designed to work only against Couchbase Server 7.1+.
//
// # UNCOMMITTED
//
// This API is UNCOMMITTED and may change in the future.
type ScopeEventingFunctionManager struct {
	getProvider func() (eventingManagementProvider, error)

	scope *Scope
}

// UpsertFunction inserts or updates an eventing function.
func (efm *ScopeEventingFunctionManager) UpsertFunction(function EventingFunction, opts *UpsertEventingFunctionOptions) error {
	if opts == nil {
		opts = &UpsertEventingFunctionOptions{}
	}

	provider, err := efm.getProvider()
	if err != nil {
		return err
	}

	return provider.UpsertFunction(efm.scope, function, opts)
}

// DropFunction drops an eventing function.
func (efm *ScopeEventingFunctionManager) DropFunction(name string, opts *DropEventingFunctionOptions) error {
	if opts == nil {
		opts = &DropEventingFunctionOptions{}
	}

	provider, err := efm.getProvider()
	if err != nil {
		return err
	}

	return provider.DropFunction(efm.scope, name, opts)
}

// DeployFunction deploys an eventing function.
func (efm *ScopeEventingFunctionManager) DeployFunction(name string, opts *DeployEventingFunctionOptions) error {
	if opts == nil {
		opts = &DeployEventingFunctionOptions{}
	}

	provider, err := efm.getProvider()
	if err != nil {
		return err
	}

	return provider.DeployFunction(efm.scope, name, opts)
}

// UndeployFunction undeploys an eventing function.
func (efm *ScopeEventingFunctionManager) UndeployFunction(name string, opts *UndeployEventingFunctionOptions) error {
	if opts == nil {
		opts = &UndeployEventingFunctionOptions{}
	}

	provider, err := efm.getProvider()
	if err != nil {
		return err
	}

	return provider.UndeployFunction(efm.scope, name, opts)
}

// GetAllFunctions fetches all the eventing functions that are in this scope.
func (efm *ScopeEventingFunctionManager) GetAllFunctions(opts *GetAllEventingFunctionsOptions) ([]EventingFunction, error) {
	if opts == nil {
		opts = &GetAllEventingFunctionsOptions{}
	}

	provider, err := efm.getProvider()
	if err != nil {
		return nil, err
	}

	return provider.GetAllFunctions(efm.scope, opts)
}

// GetFunction fetches an eventing function.
func (efm *ScopeEventingFunctionManager) GetFunction(name string, opts *GetEventingFunctionOptions) (*EventingFunction, error) {
	if opts == nil {
		opts = &GetEventingFunctionOptions{}
	}

	provider, err := efm.getProvider()
	if err != nil {
		return nil, err
	}

	return provider.GetFunction(efm.scope, name, opts)
}

// PauseFunction pauses an eventing function.
func (efm *ScopeEventingFunctionManager) PauseFunction(name string, opts *PauseEventingFunctionOptions) error {
	if opts == nil {
		opts = &PauseEventingFunctionOptions{}
	}

	provider, err := efm.getProvider()
	if err != nil {
		return err
	}

	return provider.PauseFunction(efm.scope, name, opts)
}

// ResumeFunction resumes an eventing function.
func (efm *ScopeEventingFunctionManager) ResumeFunction(name string, opts *ResumeEventingFunctionOptions) error {
	if opts == nil {
		opts = &ResumeEventingFunctionOptions{}
	}

	provider, err := efm.getProvider()
	if err != nil {
		return err
	}

	return provider.ResumeFunction(efm.scope, name, opts)
}

// FunctionsStatus fetches the current status of all eventing functions.
func (efm *ScopeEventingFunctionManager) FunctionsStatus(opts *EventingFunctionsStatusOptions) (*EventingStatus, error) {
	if opts == nil {
		opts = &EventingFunctionsStatusOptions{}
	}

	provider, err := efm.getProvider()
	if err != nil {
		return nil, err
	}

	return provider.FunctionsStatus(efm.scope, opts)
}
