package hooks

type CleanupHookMock struct {
	beforeATRGet                    func(id string) error
	beforeDocGet                    func(id string) error
	beforeRemoveLinks               func(id string) error
	beforeCommitDoc                 func(id string) error
	beforeRemoveDocStagedForRemoval func(id string) error
	beforeRemoveDoc                 func(id string) error
	beforeATRRemove                 func(id string) error
}

func (chm *CleanupHookMock) BeforeATRGet(id string) error {
	if chm.beforeATRGet == nil {
		return nil
	}
	return chm.beforeATRGet(id)
}

func (chm *CleanupHookMock) BeforeDocGet(id string) error {
	if chm.beforeDocGet == nil {
		return nil
	}
	return chm.beforeDocGet(id)
}

func (chm *CleanupHookMock) BeforeRemoveLinks(id string) error {
	if chm.beforeRemoveLinks == nil {
		return nil
	}
	return chm.beforeRemoveLinks(id)
}

func (chm *CleanupHookMock) BeforeCommitDoc(id string) error {
	if chm.beforeCommitDoc == nil {
		return nil
	}
	return chm.beforeCommitDoc(id)
}

func (chm *CleanupHookMock) BeforeRemoveDocStagedForRemoval(id string) error {
	if chm.beforeRemoveDocStagedForRemoval == nil {
		return nil
	}
	return chm.beforeRemoveDocStagedForRemoval(id)
}

func (chm *CleanupHookMock) BeforeRemoveDoc(id string) error {
	if chm.beforeRemoveDoc == nil {
		return nil
	}
	return chm.beforeRemoveDoc(id)
}

func (chm *CleanupHookMock) BeforeATRRemove(id string) error {
	if chm.beforeATRRemove == nil {
		return nil
	}
	return chm.beforeATRRemove(id)
}
