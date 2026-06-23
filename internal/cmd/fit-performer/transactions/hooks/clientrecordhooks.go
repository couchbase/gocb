package hooks

type ClientRecordHookMock struct {
	beforeCreateRecord func() error
	beforeRemoveClient func() error
	beforeUpdateCAS    func() error
	beforeGetRecord    func() error
	beforeUpdateRecord func() error
}

func (crhm *ClientRecordHookMock) BeforeCreateRecord() error {
	if crhm.beforeCreateRecord == nil {
		return nil
	}
	return crhm.beforeCreateRecord()
}

func (crhm *ClientRecordHookMock) BeforeRemoveClient() error {
	if crhm.beforeRemoveClient == nil {
		return nil
	}
	return crhm.beforeRemoveClient()
}

func (crhm *ClientRecordHookMock) BeforeUpdateCAS() error {
	if crhm.beforeUpdateCAS == nil {
		return nil
	}
	return crhm.beforeUpdateCAS()
}

func (crhm *ClientRecordHookMock) BeforeGetRecord() error {
	if crhm.beforeGetRecord == nil {
		return nil
	}
	return crhm.beforeGetRecord()
}

func (crhm *ClientRecordHookMock) BeforeUpdateRecord() error {
	if crhm.beforeUpdateRecord == nil {
		return nil
	}
	return crhm.beforeUpdateRecord()
}
